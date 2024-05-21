package slab

import (
	"encoding/binary"
)

const (
	slabIndexSize    = 4
	slabBufIndexSize = 4
	slabFooterSize   = slabIndexSize + slabBufIndexSize // 8 B

)

type slab struct {
	refCount        uint
	initialisedBufs uint
	avail           uint
	chunkSize       uint
	index           uint
	// list of buffers, a singly linked list
	// the buffers might span-out into multiple pages
	buffers       []byte
	freeStack     []uint
	freeStackHead int
}

func newSlab(chunkSize, pageSize, index uint) *slab {

	var pageCount uint = 0
	// We are going to acquire this footer space also
	effectiveChunkSize := chunkSize + slabFooterSize
	var chunkCount uint = pageSize / effectiveChunkSize

	if chunkCount > 0 { // for small allocation we would just allocate one page
		pageCount = 1
	} else { // for large objects we would declare as many pages as wanted for one object
		pageCount = chunkCount + 1
	}

	allocatedBytes := make([]byte, pageCount*pageSize)
	var slb = slab{
		buffers:       allocatedBytes,
		avail:         chunkCount,
		chunkSize:     chunkSize,
		index:         index,
		freeStack:     make([]uint, chunkCount),
		freeStackHead: -1,
	}
	return &slb
}

func (s *slab) nextBuf() []byte {
	if s.initialisedBufs < s.avail {
		return nil
	}
	buf := s.retriveBuf(s.initialisedBufs)
	addFooter(buf, s.initialisedBufs, s.index)
	s.initialisedBufs++
	return buf
}

func (s *slab) retriveBuf(index uint) []byte {
	effectiveChunkSize := s.chunkSize + slabFooterSize
	startIndex := index * effectiveChunkSize
	endIndex := (index + 1) * effectiveChunkSize

	buf := s.buffers[startIndex:endIndex]
	return buf
}

func (s *slab) get() []byte {

	if s.freeStackHead >= 0 {
		buf := s.retriveBuf(s.freeStack[s.freeStackHead])
		s.freeStackHead -= 1
		s.refCount++
		return buf
	}
	buf := s.nextBuf()
	if buf != nil {
		s.refCount++
	}
	return buf
}

func (s *slab) put(buf []byte) {
	bufIndex := readBufIndex(buf)

	s.freeStackHead += 1
	// Adding buffer to the head of freelist
	s.freeStack[s.freeStackHead] = bufIndex
	s.refCount--
}

func addFooter(buf []byte, bufIndex, index uint) {
	footerStaringIndex := len(buf) - slabFooterSize
	binary.BigEndian.PutUint32(buf[footerStaringIndex:], uint32(index))
	binary.BigEndian.PutUint32(buf[footerStaringIndex+slabIndexSize:], uint32(bufIndex))
}

func readBufIndex(buf []byte) uint {
	footerStaringIndex := len(buf) - slabFooterSize
	return uint(binary.BigEndian.Uint32(buf[footerStaringIndex+slabIndexSize:]))
}
func readSlabIndex(buf []byte) uint {
	footerStaringIndex := len(buf) - slabFooterSize
	return uint(binary.BigEndian.Uint32(buf[footerStaringIndex : footerStaringIndex+slabIndexSize]))
}

func (s *slab) isEmpty() bool {
	return s.refCount == 0
}

func (s *slab) isFull() bool {
	return s.initialisedBufs == s.avail && s.freeStackHead < 0
}

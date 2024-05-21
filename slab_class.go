package slab

import (
	"container/ring"
	"sync"
)

type freeList struct {
	nextFreeSlab, head, tail *ring.Ring
	register                 map[uint]*ring.Ring
}

func newFreeList() *freeList {
	return &freeList{
		register: make(map[uint]*ring.Ring),
	}
}

func (f *freeList) Len() int {
	return f.nextFreeSlab.Len()
}

func (f *freeList) addFreeSlab(slb *slab) {
	node := ring.New(1)
	node.Value = slb
	node.Link(f.nextFreeSlab)
	f.register[slb.index] = node
	f.nextFreeSlab = node
}

func (f *freeList) findFreeSlab() *slab {
	if f.nextFreeSlab.Len() > 0 {
		targetSlab := f.nextFreeSlab.Value.(*slab)
		if targetSlab.isFull() {
			return nil
		}
		// empty or partially filled
		return targetSlab
	}
	return nil
}

func (f *freeList) getSlab(index uint) *slab {
	slb, ok := f.register[index]
	if ok {
		return slb.Value.(*slab)
	}
	return nil
}

type slabClass struct {
	chunkSize, pageSize uint
	lock                sync.Mutex
	freeList            *freeList
	maxSlabId           uint
}

func (s *slabClass) get() []byte {
	s.lock.Lock()
	defer s.lock.Unlock()

	slab := s.freeList.findFreeSlab()
	if slab == nil {
		slab = s.addSlab()
	}
	return s.get()
}

func (s *slabClass) addSlab() *slab {
	s.maxSlabId++
	slab := newSlab(s.chunkSize, s.pageSize, s.maxSlabId)
	s.freeList.addFreeSlab(slab)
	return slab
}

func (s *slabClass) put(buf []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	index := readSlabIndex(buf)
	slab := s.freeList.getSlab(index)
	if slab == nil {
		panic("slab not found for buffer")
	}
	slab.put(buf)

}

func (s *slabClass) refreshFreeList(slb *slab, node *ring.Ring) {
	if slb.isEmpty() {
		// slab got empty after last put
		// Lets put this node on tail
		var probingNode *ring.Ring = node
		var probingSlab *slab
		var movement int
		var noFullSlab bool
		for probingSlab = probingNode.Value.(*slab); probingSlab.isFull(); probingSlab = probingNode.Value.(*slab) {

			probingNode = probingNode.Next()
			if node == probingNode {
				noFullSlab = true
			}
			movement++
		}
		node.Move(movement) // moving newly empty node to the last of the list
	}
}

package rtree

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
)

type FreeList struct {
	mutex sync.Mutex
	nodes []*node
	size  int
}

type copyOnWriteContext struct {
	freelist *FreeList
}

type children []*node

type node struct {
	value    interface{}
	cow      *copyOnWriteContext
	children children
	prefix   []byte
}

type Tree struct {
	cow      *copyOnWriteContext
	children children
}

func NewFreeList(size int) *FreeList {
	return &FreeList{
		mutex: sync.Mutex{},
		nodes: make([]*node, 0, size),
		size:  size,
	}
}

func (c *copyOnWriteContext) newNode() *node {
	n := c.freelist.newNode()
	n.cow = c

	return n
}

func (freelist *FreeList) newNode() *node {
	var n *node
	freelist.mutex.Lock()
	if size := len(freelist.nodes); size != 0 {
		n = freelist.nodes[size-1]
		freelist.nodes[size-1] = nil
		freelist.nodes = freelist.nodes[:size-1]
	}
	freelist.mutex.Unlock()
	if n == nil {
		return new(node)
	}
	return n
}

func (freelist *FreeList) freeNode(node *node) {
	freelist.mutex.Lock()
	if size := len(freelist.nodes); size < freelist.size {
		freelist.nodes = append(freelist.nodes, node)
	}
	freelist.mutex.Lock()
}

var DefaultFreeListSize = 32

func New() *Tree {
	return &Tree{
		cow: &copyOnWriteContext{freelist: NewFreeList(DefaultFreeListSize)},
	}
}

func (tree *Tree) Clone() *Tree {
	clone := *tree
	cow1, cow2 := *tree.cow, *tree.cow
	clone.children = make(children, len(tree.children))
	copy(clone.children, tree.children)
	clone.cow = &cow1
	tree.cow = &cow2
	return &clone
}

func newRNode(cow *copyOnWriteContext, prefix []byte, value interface{}) *node {
	n := cow.newNode()
	n.prefix = prefix
	n.value = value
	return n
}

func (children children) Print() {
	for _, child := range children {
		fmt.Println(child.prefix)
	}
}

func (children children) findNode(first byte) (int, *node) {
	i := sort.Search(len(children), func(i int) bool {
		return first < children[i].prefix[0]
	})
	if i > 0 && children[i-1].prefix[0] == first {
		return i - 1, children[i-1]
	}
	return i, nil
}

func (children *children) insetAt(node *node, index int) {
	*children = append(*children, nil)
	if index < len(*children) {
		copy((*children)[index+1:], (*children)[index:])
	}
	(*children)[index] = node
}

func (children *children) deleteAt(index int) {
	copy((*children)[index:], (*children)[index+1:])
	(*children)[len(*children)-1] = nil
	*children = (*children)[:len(*children)-1]
}

func (children *children) delete(cow *copyOnWriteContext, key []byte) {
	if index, child := children.findNode(key[0]); child != nil {
		child = children.mutableChild(cow, index)
		if len(key) < len(child.prefix) ||
			bytes.Compare(key[:len(child.prefix)], child.prefix) != 0 {
			return
		}
		if len(child.prefix) == len(key) {
			if len(child.children) == 0 {
				children.deleteAt(index)
				child.prefix = nil
			} else {
				if len(child.children) == 1 {
					child.merge()
				} else {
					child.value = nil
				}
			}
			return
		}
		if len(child.children) == 0 {
			return
		}
		child.children.delete(cow, key[len(child.prefix):])
		if len(child.children) == 1 && child.value == nil {
			child.merge()
		}
	}
}

func (children *children) mutableChild(cow *copyOnWriteContext, index int) *node {
	c := (*children)[index]
	if c.cow != cow {
		c = c.mutableFor(cow)
		(*children)[index] = c
	}
	return c
}

func (children children) walk(stack [][]byte, f func(prefixes [][]byte, value interface{}) bool) bool {
	for _, child := range children {
		if child.value != nil {
			if f(append(stack, child.prefix), child.value) == false {
				return false
			}
		}
		if child.children.walk(append(stack, child.prefix), f) == false {
			return false
		}
	}
	return true
}

func (n *node) find(key []byte) bool {
	if n.value != nil && bytes.Compare(n.prefix, key) == 0 {
		return true
	}
	key = key[len(n.prefix):]
	if _, child := n.children.findNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func (tree *Tree) Find(key []byte) bool {
	if tree.children == nil || len(key) == 0 {
		return false
	}
	if _, child := tree.children.findNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func bytesCopy(data []byte) []byte {
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func (tree *Tree) ReplaceOrInsert(key []byte, val interface{}) interface{} {
	if len(key) == 0 || val == nil {
		return nil
	}
	index, child := tree.children.findNode(key[0])
	if child == nil {
		tree.children.insetAt(newRNode(tree.cow, key, val), index)
	} else {
		return tree.children.mutableChild(tree.cow, index).replaceOrInsert(key, val)
	}
	return nil
}

var Empty = []byte{'e', 'm', 'p', 't', 'y'}

func (tree *Tree) Insert(key []byte) {
	if len(key) == 0 {
		return
	}
	index, child := tree.children.findNode(key[0])
	if child == nil {
		tree.children.insetAt(newRNode(tree.cow, key, Empty), index)
	} else {
		tree.children.mutableChild(tree.cow, index).replaceOrInsert(key, Empty)
	}
}

func (tree *Tree) Delete(key []byte) {
	tree.children.delete(tree.cow, key)
}

func (tree Tree) Walk(f func(prefixes [][]byte, val interface{}) bool) {
	tree.children.walk(make([][]byte, 0, 32), f)
}

func prefixLen(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func (n *node) mutableFor(cow *copyOnWriteContext) *node {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children, len(n.children), cap(n.children))
	}
	if len(out.children) > 0 {
		copy(out.children, n.children)
	}
	out.prefix = bytesCopy(n.prefix)
	out.value = n.value
	return out
}

func (n *node) mutableChild(i int) *node {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

func printTokens(bytesTokens [][]byte) {
	var tokens []string
	for _, token := range bytesTokens {
		tokens = append(tokens, string(token))
	}
	fmt.Println(tokens)
}

func (n *node) replaceOrInsert(key []byte, val interface{}) interface{} {
	if bytes.Compare(n.prefix, key) == 0 {
		if n.value == nil {
			n.value = val
			return nil
		}
		old := n.value
		n.value = val
		return old
	}
	index := prefixLen(n.prefix, key)
	if index == len(n.prefix) {
		key = key[index:]
		index, child := n.children.findNode(key[0])
		if child == nil {
			n.children.insetAt(newRNode(n.cow, bytesCopy(key), val), index)
		} else {
			return n.mutableChild(index).replaceOrInsert(key, val)
		}
	} else {
		child := *n
		child.prefix = n.prefix[index:]
		n.value = nil
		n.prefix = n.prefix[:index]
		n.children = make(children, 1, 2)
		n.children[0] = &child
		key = key[index:]
		if len(key) > 0 {
			index, _ := n.children.findNode(key[0])
			n.children.insetAt(newRNode(n.cow, bytesCopy(key), val), index)
		} else {
			n.value = val
		}
	}
	return nil
}

func (n *node) merge() {
	prefix := make([]byte, len(n.prefix)+len(n.children[0].prefix))
	n.value = n.children[0].value
	copy(prefix, n.prefix)
	copy(prefix[len(n.prefix):], n.children[0].prefix)
	n.prefix = prefix
	old := n.children
	n.children = n.children[0].children
	old[0] = nil
}

func (n *node) findNode(b byte) (int, *node) {
	return n.children.findNode(b)
}

type stackItem struct {
	*node
	visit bool
}

type stack struct {
	stack []*stackItem
}

func (s *stack) peek() *stackItem {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	return rNode
}

func (s *stack) pop() *stackItem {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return rNode
}

func (s *stack) push(children ...*node) {
	for i := len(children) - 1; i >= 0; i-- {
		s.stack = append(s.stack, &stackItem{node: children[i]})
	}
}

const (
	PushKey = '='
	Push    = '+'
	Pop     = '-'
)

func (tree *Tree) WriteToWithGzip(writer io.Writer, marshaler func(interface{}) ([]byte, error)) (int64, error) {
	gzipWriter := gzip.NewWriter(writer)
	n, err := tree.WriteTo(gzipWriter, marshaler)
	if err != nil {
		return 0, err
	}
	if err := gzipWriter.Flush(); err != nil {
		return 0, err
	}
	if err := gzipWriter.Close(); err != nil {
		return 0, err
	}
	return n, nil
}

func (tree *Tree) WriteTo(writer io.Writer, marshaler func(interface{}) ([]byte, error)) (int64, error) {
	var size int64
	var stack stack
	var buffer bytes.Buffer
	var pop = []byte{Pop}
	var lenBuf [4]byte
	stack.push(tree.children...)
	for item := stack.peek(); item != nil; item = stack.peek() {
		visit := item.visit
		item.visit = true
		if visit == false {
			buffer.Reset()
			if item.value != nil {
				buffer.WriteByte(PushKey)
			} else {
				buffer.WriteByte(Push)
			}

			//write prefix
			n := binary.PutVarint(lenBuf[:], int64(len(item.prefix)))
			buffer.Write(lenBuf[:n])
			buffer.Write(item.prefix)

			//write val
			if item.value != nil {
				data, err := marshaler(item.value)
				if err != nil {
					return 0, err
				}
				n := binary.PutVarint(lenBuf[:], int64(len(data)))
				buffer.Write(lenBuf[:n])
				buffer.Write(data)
			}

			if n, err := writer.Write(buffer.Bytes()); err != nil {
				return 0, err
			} else {
				size += int64(n)
			}
			if item.children != nil {
				stack.push(item.children...)
				continue
			}
		}
		stack.pop()
		if n, err := writer.Write(pop); err != nil {
			return 0, err
		} else {
			size += int64(n)
		}
	}
	return size, nil
}

func ReBuildTreeWithGzip(reader io.Reader, unMarshal func(data []byte) (interface{}, error)) (*Tree, error) {
	reader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return ReBuildTree(reader, unMarshal)
}

func ReBuildTree(reader io.Reader, unMarshal func(data []byte) (interface{}, error)) (*Tree, error) {
	type OpCode struct {
		op     byte
		prefix []byte
		value  interface{}
	}

	const bufferSize = 1 << 10
	var tree = New()
	var stack = make([]*children, 0, 128)
	var curr *children
	var opCodesCh = make(chan []OpCode, 4<<10)
	var opCodes = make([]OpCode, 0, bufferSize)
	var err error
	bufReader := bufio.NewReader(reader)

	readBytes := func() []byte {
		var size int64
		size, err = binary.ReadVarint(bufReader)
		if err != nil {
			return nil
		}
		prefix := make([]byte, size)
		if _, err = io.ReadFull(bufReader, prefix); err != nil {
			return nil
		}
		return prefix
	}

	go func() {
		defer func() {
			close(opCodesCh)
		}()
		var opCode [1]byte
		for {
			if _, err := bufReader.Read(opCode[:]); err != nil {
				if err == io.EOF {
					break
				}
			}
			switch opCode[0] {
			case Pop:
				opCodes = append(opCodes, OpCode{op: Pop})
			case Push, PushKey:
				prefix := readBytes()
				if prefix == nil {
					return
				}
				if opCode[0] == Push {
					opCodes = append(opCodes, OpCode{op: Push, prefix: prefix})
					break
				}
				data := readBytes()
				if data == nil {
					return
				}
				var val interface{}
				val, err = unMarshal(data)
				if err != nil {
					return
				}
				opCodes = append(opCodes, OpCode{op: Push, prefix: prefix, value: val})
			}
			if len(opCodes) < bufferSize {
				continue
			}
			opCodesCh <- opCodes
			opCodes = make([]OpCode, 0, bufferSize)
		}
		opCodesCh <- opCodes
	}()
	for tokens := range opCodesCh {
		for _, opCode := range tokens {
			if opCode.op == PushKey || opCode.op == Push {
				if len(stack) == 0 {
					stack = append(stack, &tree.children)
					curr = &tree.children
				} else {
					stack = append(stack, curr)
				}
				if curr == nil {
					return nil, fmt.Errorf("stack error")
				}
				next := newRNode(tree.cow, opCode.prefix, opCode.value)
				*curr = append(*curr, next)
				curr = &next.children
			} else if opCode.op == Pop {
				if len(stack) == 0 {
					return nil, fmt.Errorf("stack error")
				}
				curr = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			} else {
				return nil, fmt.Errorf("unkown opCode %c", opCode.op)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("broken stack")
	}
	return tree, nil
}

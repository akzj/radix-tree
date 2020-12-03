package rtree

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
)

type Children []*RNode

type RNode struct {
	visit    bool
	key      bool
	children Children
	prefix   []byte
}

type RTree struct {
	children Children
}

func newRNode(prefix []byte, key bool) *RNode {
	return &RNode{
		key:    key,
		prefix: prefix,
	}
}

func (children Children) walkWithStack(stack [][]byte, f func(key [][]byte) bool) bool {
	for _, it := range children {
		if it.key {
			if f(append(stack, it.prefix)) == false {
				return false
			}
		}
		if it.children != nil {
			if it.children.walkWithStack(append(stack, it.prefix), f) == false {
				return false
			}
		}
	}
	return true
}

func (children Children) Print() {
	for _, child := range children {
		fmt.Println(child.prefix)
	}
}

func (children Children) FindNode(first byte) (int, *RNode) {
	i := sort.Search(len(children), func(i int) bool {
		return first < children[i].prefix[0]
	})
	if i > 0 && children[i-1].prefix[0] == first {
		return i, children[i-1]
	}
	return i, nil
}

func (children *Children) insetAt(node *RNode, index int) {
	*children = append(*children, nil)
	if index < len(*children) {
		copy((*children)[index+1:], (*children)[index:])
	}
	(*children)[index] = node
}

func (children *Children) deleteAt(index int) {
	copy((*children)[index:], (*children)[index+1:])
	*children = (*children)[:len(*children)-1]
}

func (children *Children) delete(key []byte) {
	if index, child := children.FindNode(key[0]); child != nil {
		if bytes.Compare(child.prefix, key) == 0 {
			if len(child.children) == 0 {
				children.deleteAt(index - 1)
				return
			} else {
				if len(child.children) == 1 {
					prefix := make([]byte, len(child.prefix)+len(child.children[0].prefix))
					child.key = child.children[0].key
					copy(prefix, child.prefix)
					copy(prefix[len(child.prefix):], child.children[0].prefix)
					child.children = child.children[0].children
				} else {
					child.key = false
				}
				return
			}
		}
		if child.children == nil {
			return
		}
		child.children.delete(key[len(child.prefix):])
		//merge
		if len(child.children) == 1 && child.key == false {
			//fmt.Println("-- merge -- ")
			prefix := make([]byte, len(child.prefix)+len(child.children[0].prefix))
			child.key = child.children[0].key
			copy(prefix, child.prefix)
			copy(prefix[len(child.prefix):], child.children[0].prefix)
			child.children = child.children[0].children
		}
	}
}

func (node *RNode) find(key []byte) bool {
	if node.key && bytes.Compare(node.prefix, key) == 0 {
		return true
	}
	key = key[len(node.prefix):]
	if _, child := node.children.FindNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func (tree *RTree) Find(key []byte) bool {
	if _, child := tree.children.FindNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func (tree *RTree) Insert(key []byte) {
	if len(key) == 0 {
		return
	}
	index, child := tree.children.FindNode(key[0])
	if child == nil {
		k := make([]byte, len(key))
		copy(k, key)
		tree.children.insetAt(newRNode(k, true), index)
	} else {
		child.insert(key)
	}
}

func (tree *RTree) Delete(key []byte) {
	if tree.children != nil && len(key) != 0 {
		tree.children.delete(key)
	}
}

func (tree RTree) Walk(f func(prefixes [][]byte) bool) {
	tree.children.walkWithStack(make([][]byte, 0, 32), f)
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

func (node *RNode) insert(key []byte) {
	if bytes.Compare(node.prefix, key) == 0 {
		if node.key == false {
			node.key = true
		} else {
			//fmt.Println("repeated")
		}
		return
	}
	index := prefixLen(node.prefix, key)
	if index == len(node.prefix) {
		key = key[index:]
		index, child := node.children.FindNode(key[0])
		if child == nil {
			k := make([]byte, len(key))
			copy(k, key)
			node.children.insetAt(newRNode(k, true), index)
		} else {
			child.insert(key)
		}
	} else {
		child := *node
		child.prefix = node.prefix[index:]
		node.prefix = node.prefix[:index]
		node.children = make(Children, 1)
		node.children[0] = &child
		node.key = false
		key = key[index:]
		if len(key) > 0 {
			k := make([]byte,len(key))
			copy(k, key)
			rNode := newRNode(k, true)
			index, _ := node.children.FindNode(key[0])
			node.children.insetAt(rNode, index)
		} else {
			node.key = true
		}
	}
}

type Stack struct {
	stack Children
}

func (s *Stack) peek() *RNode {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	return rNode
}

func (s *Stack) pop() *RNode {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return rNode
}

func (s *Stack) push(children Children) {
	for i := len(children) - 1; i >= 0; i-- {
		s.stack = append(s.stack, children[i])
	}
}

const (
	PushKey = '='
	Push    = '+'
	Pop     = '-'
)

func (tree RTree) WriteTo(writer io.Writer) (int64, error) {
	var size int64
	var stack = Stack{}
	var text bytes.Buffer
	stack.push(tree.children)
	for rNode := stack.peek(); rNode != nil; rNode = stack.peek() {
		visit := rNode.visit
		rNode.visit = true
		if visit == false {
			text.Reset()
			if rNode.key {
				text.WriteByte(PushKey)
			} else {
				text.WriteByte(Push)
			}
			text.Write(rNode.prefix)
			text.WriteByte('\n')
			if n, err := writer.Write(text.Bytes()); err != nil {
				return 0, err
			} else {
				size += int64(n)
			}
		}
		if visit == false && rNode.children != nil {
			stack.push(rNode.children)
			continue
		}
		stack.pop()
		if n, err := writer.Write([]byte(string(Pop) + "\n")); err != nil {
			return 0, err
		} else {
			size += int64(n)
		}
	}
	return size, nil
}

func FastBuildTree(reader io.Reader) (*RTree, error) {
	const bufferSize = 1 << 10
	var tree RTree
	var stack = make([]*Children, 0, 128)
	var curr *Children
	var scanner = bufio.NewScanner(reader)
	var tokens = make(chan [][]byte, 4<<10)
	var buffer = make([][]byte, 0, bufferSize)

	go func() {
		for scanner.Scan() {
			data := scanner.Bytes()
			k := make([]byte, len(data))
			copy(k, data)
			buffer = append(buffer, k)
			if len(buffer) < bufferSize {
				continue
			}
			tokens <- buffer
			buffer = make([][]byte, 0, bufferSize)
		}
		if len(buffer) != 0 {
			tokens <- buffer
		}
		close(tokens)
	}()
	for token := range tokens {
		for _, token := range token {
			if token[0] == PushKey || token[0] == Push {
				if len(stack) == 0 {
					stack = append(stack, &tree.children)
					curr = &tree.children
				} else {
					stack = append(stack, curr)
				}
				if curr == nil {
					return nil, fmt.Errorf("stack error")
				}
				next := newRNode(token[1:], token[0] == PushKey)
				*curr = append(*curr, next)
				curr = &next.children
			} else {
				curr = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("broken stack")
	}
	return &tree, nil

}

func BuildTree(reader io.Reader) (*RTree, error) {
	var tree RTree
	var stack []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		text := scanner.Text()
		if text[0] == PushKey || text[0] == Push {
			stack = append(stack, text[1:])
			if text[0] == PushKey {
				tree.Insert([]byte(strings.Join(stack, "")))
			}
		} else {
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("broken stack")
	}
	return &tree, nil
}

package rtree

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/google/btree"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	sort "sort"
	"testing"
	"time"
)


func printMemUsed() {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		panic(err)
	}
	runtime.GC()
	m, _ := p.MemoryInfo()
	fmt.Println("MemoryInfo", m.RSS>>20)
}

func TestInsert(t *testing.T) {
	keys := []string{
		"acccc",
		"bcccc",
	}
	var tree = New()
	for _, it := range keys {
		tree.Insert([]byte(it))
	}
	tree.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})

	print("-----------\n")
	clone := tree.Clone()

	clone.Insert([]byte("acccb"))
	clone.Insert([]byte("bcccb"))
	clone.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})
	print("-----------\n")
	tree.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})
}

func TestRTreeDeleteMerge(t *testing.T) {
	var tree = New()
	tree.Insert([]byte("aaa"))
	tree.Insert([]byte("aaabbb"))
	tree.Insert([]byte("aaaccc"))
	tree.Insert([]byte("aaacccbbb"))
	tree.Insert([]byte("aaacccddd"))

	tree.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})
	fmt.Println("-------------------")
	tree.Delete([]byte("aaa"))
	tree.Delete([]byte("aaaccc"))
	tree.Delete([]byte("aaacccbbb"))
	tree.Delete([]byte("aaabbb"))

	tree.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})
}

func TestChildrenDelete(t *testing.T) {
	cases := []struct {
		keys []string
	}{
		{
			keys: []string{
				"a",
				"acc",
				"acccc",
				"ac",
			},
		},
	}

	for _, Case := range cases {
		var tree = New()
		for _, val := range Case.keys {
			fmt.Println(string(val))
			tree.Insert([]byte(val))
		}
		tree.Walk(func(prefixes [][]byte) bool {
			printTokens(prefixes)
			return true
		})
		fmt.Println("---------------")
		for _, val := range Case.keys {
			fmt.Println("delete", val)
			tree.Delete([]byte(val))
		}
		tree.Walk(func(prefixes [][]byte) bool {
			printTokens(prefixes)
			return true
		})
	}
}

func TestWriteRBtreeDelete(t *testing.T) {
	f, err := os.Open("../files.txt")
	if err != nil {
		t.Fatal(err.Error())
	}
	scanner := bufio.NewScanner(bufio.NewReader(f))
	var tree = New()
	var keys [][]byte
	for ; scanner.Scan(); {
		text := scanner.Bytes()
		data := make([]byte, len(text))
		copy(data, text)
		tree.Insert(data)
		keys = append(keys, data)
	}
	f.Close()
	f = nil
	scanner = nil
	printMemUsed()
	begin := time.Now()
	count := len(keys)
	for index, key := range keys {
		tree.Delete(key)
		keys[index] = nil
	}
	fmt.Println("delete/s ", int(float64(count)/time.Now().Sub(begin).Seconds()))
	tree.Walk(func(prefixes [][]byte) bool {
		printTokens(prefixes)
		return true
	})
	printMemUsed()
}

func TestChildrenInsert(t *testing.T) {

	cases := []struct {
		keys []string
	}{
		{
			keys: []string{
				"acccc",
				"accccbbbb",
				"acccbbb",
				"acccabb",
				"acc",
				"accc",
				"bccc",
				"b1cc",
			},
		},
		{
			keys: []string{
				"acc",
				"accc",
				"acccc",
				"accccc",
				"acccb",
				"accb",
				"acb",
			},
		},
	}

	for _, Case := range cases {
		vals := Case.keys
		var tree = New()
		for _, val := range vals {
			tree.Insert([]byte(val))
		}
		var result []string
		tree.Walk(func(key [][]byte) bool {
			result = append(result, string(bytes.Join(key, nil)))
			printTokens(key)
			return true
		})
		sort.Strings(result)
		sort.Strings(vals)
		if reflect.DeepEqual(result, vals) == false {
			t.Errorf("test case failed \n%+v,\n%+v", result, vals)
		}
		for _, val := range vals {
			if tree.Find([]byte(val)) == false {
				t.Errorf("no find key:%s", val)
			}
		}
		tree.Walk(func(prefixes [][]byte) bool {
			printTokens(prefixes)
			return true
		})
		var buffer bytes.Buffer
		tree.WriteTo(&buffer)

		fmt.Println("---------------")
		fmt.Println(buffer.String())
		fmt.Println("---------------")

		if tree2, err := FastBuildTree(&buffer); err == nil {
			tree2.Walk(func(prefixes [][]byte) bool {
				printTokens(prefixes)
				return true
			})
		} else {
			t.Errorf(err.Error())
		}
	}
}

func TestWriteRBtree(t *testing.T) {
	f, err := os.Open("../files.txt")
	if err != nil {
		t.Fatal(err.Error())
	}
	scanner := bufio.NewScanner(bufio.NewReader(f))
	var tree = New()
	for ; scanner.Scan(); {
		text := scanner.Bytes()
		tree.Insert(text)
	}
	f.Close()

	begin := time.Now()
	var buffer bytes.Buffer
	if _, err := tree.WriteTo(&buffer); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("WriteTo tree token seconds %0.3f\n", time.Now().Sub(begin).Seconds())
	if err := ioutil.WriteFile("rtree.stack", buffer.Bytes(), 0666); err != nil {
		t.Fatal(err.Error())
	}

	writeString := func(tree *RTree, filename string) {
		begin := time.Now()
		defer func() {
			fmt.Printf("write tree token seconds %0.3f", time.Now().Sub(begin).Seconds())
		}()
		var buffer bytes.Buffer
		tree.Walk(func(prefixes [][]byte) bool {
			buffer.Write(append(bytes.Join(prefixes, nil), '\n'))
			return true
		})
		if err := ioutil.WriteFile(filename, buffer.Bytes(), 0666); err != nil {
			t.Fatal(err.Error())
		}
	}
	writeString(tree, "stack.txt")
	{
		f, err := os.Open("rtree.stack")
		if err != nil {
			t.Fatal(err.Error())
		}
		tree, err := FastBuildTree(f)
		if err != nil {
			t.Fatal(err)
		}
		writeString(tree, "stack.txt2")
	}

}

/*
1195
take time seconds 3.255--- PASS: TestReloadRtree (3.26s)
PASS
*/

/*
1524
take time seconds 8.639--- PASS: TestReloadRtree (8.64s)
PASS
*/
func TestReloadRtree(t *testing.T) {
	f, err := os.Open("rtree.stack")
	if err != nil {
		t.Fatal(err.Error())
	}
	begin := time.Now()
	if _, err := FastBuildTree(bufio.NewReaderSize(f, 1<<20)); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("take time seconds %0.3f\n", time.Now().Sub(begin).Seconds())
}

type text []byte

func newText(data []byte) text {
	t := make([]byte, len(data))
	copy(t, data)
	return t
}
func (t text) Less(than btree.Item) bool {
	return bytes.Compare(t, than.(text)) < 0
}

func TestBtree(t *testing.T) {
	tree := btree.New(3)
	f, err := os.Open("../files.txt")
	if err != nil {
		t.Fatal(err.Error())
	}
	printMemUsed()
	scanner := bufio.NewScanner(bufio.NewReader(f))
	var count int
	begin := time.Now()
	for ; scanner.Scan(); {
		text := scanner.Bytes()
		if len(text) > 0 {
			tree.ReplaceOrInsert(newText(text))
			count++
		}
	}
	f.Close()
	fmt.Println("done ", count)
	fmt.Println("insert/s", int(float64(count)/time.Now().Sub(begin).Seconds()))

	printMemUsed()

	data, _ := ioutil.ReadFile("../files.txt")

	count = 0
	tokens := bytes.Split(data, []byte("\n"))
	begin = time.Now()
	for _, str := range tokens {
		if len(str) > 0 {
			if tree.Get(text(str)) == nil {
				t.Errorf("no find key:%v", str)
			}
			count++
		}
		if count%100000 == 0 {
			fmt.Println(int(float64(count) / time.Now().Sub(begin).Seconds()))
		}
	}
	fmt.Println("find done ", count)
	fmt.Println("find/s", int(float64(count)/time.Now().Sub(begin).Seconds()))
}

/*
insert/s 1269929
find/s 1965258
*/

func TestLoadFile(t *testing.T) {
	f, err := os.Open("../files.txt")
	if err != nil {
		t.Fatal(err.Error())
	}
	scanner := bufio.NewScanner(bufio.NewReader(f))
	var tree = New()
	var count int
	begin := time.Now()
	printMemUsed()
	for ; scanner.Scan(); {
		text := scanner.Bytes()
		if len(text) > 0 {
			tree.Insert(text)
			count++
		}
	}
	f.Close()
	fmt.Println("done ", count)
	fmt.Println("insert/s", int(float64(count)/time.Now().Sub(begin).Seconds()))
	printMemUsed()


	data, _ := ioutil.ReadFile("../files.txt")

	count = 0
	tokens := bytes.Split(data, []byte("\n"))
	begin = time.Now()
	for _, str := range tokens {
		if len(str) > 0 {
			if tree.Find(str) == false {
				t.Errorf("no find key:%v", str)
			}
			count++
		}
		if count%100000 == 0 {
			fmt.Println(int(float64(count) / time.Now().Sub(begin).Seconds()))
		}
	}
	fmt.Println("find done ", count)
	fmt.Println("find/s", int(float64(count)/time.Now().Sub(begin).Seconds()))
}

/*
BenchmarkCopy
BenchmarkCopy-12    	1000000000	         0.252 ns/op
*/
func BenchmarkCopy(b *testing.B) {
	var data = make([]byte, 64)
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 64)
		copy(buffer, data)
	}
}

/*
BenchmarkAppend
BenchmarkAppend-12    	16097245	        65.8 ns/op
*/
func BenchmarkAppend(b *testing.B) {
	var data = make([]byte, 64)
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 64)
		buffer = append(buffer, data...)
	}
}

func TestGzipTest(t *testing.T) {
	out, err := os.Create("files.gz")
	if err != nil {
		t.Fatal(err.Error())
	}
	writer := gzip.NewWriter(out)

	in, err := os.Open("../files.txt")
	if err != nil {
		t.Fatal(err.Error())
	}
	if _, err := io.Copy(writer, in); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}


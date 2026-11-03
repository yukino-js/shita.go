package skiplist_test

import (
	"fmt"
	"strings"

	skiplist "github.com/yukino-js/shita.go/yukino-skiplist"
)

func ExampleNew() {
	list := skiplist.New[int, string]()
	list.Insert(3, "algorithms")
	list.Insert(1, "learn")
	// Insert refuses to replace a value. Set reports what it replaced.
	list.Insert(3, "ignored")
	previous, existed := list.Set(1, "study")
	fmt.Printf("replaced %q (existed=%t)\n", previous, existed)
	for key, value := range list.All() {
		fmt.Printf("%d=%s\n", key, value)
	}
	// Output:
	// replaced "learn" (existed=true)
	// 1=study
	// 3=algorithms
}

func ExampleLoad() {
	const dump = "1:learn\n3:algorithms\n"
	format := skiplist.NewFormat(skiplist.IntCodec(), skiplist.StringCodec())
	list, err := skiplist.Load[int, string](strings.NewReader(dump), format)
	if err != nil {
		panic(err)
	}
	value, ok := list.Get(3)
	fmt.Printf("%q %t\n", value, ok)
	// Output:
	// "algorithms" true
}

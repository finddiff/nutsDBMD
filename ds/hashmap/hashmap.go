package hashmap

import "sync"

type hashmap struct {
	//dsmap map[string]interface{}
	dsmap sync.Map
}

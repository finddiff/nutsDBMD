package bytetree

//type ValueLink struct {
//	Value interface{}
//	Pre   *ValueLink
//	Next  *ValueLink
//}
//
//func makeValueLink(value interface{}) *ValueLink {
//	return &ValueLink{
//		Value: value,
//		Pre:   nil,
//		Next:  nil,
//	}
//}

type IndexNode struct {
	KeyChat   uint8
	Level     uint16
	Parent    *IndexNode
	FirstCh   *IndexNode
	LastCh    *IndexNode
	MilCh     *IndexNode
	Pre       *IndexNode
	Next      *IndexNode
	ValueLink *LinkNode
}

func initNode() *IndexNode {
	return &IndexNode{
		Parent:    nil,
		KeyChat:   0,
		Level:     0,
		FirstCh:   nil,
		LastCh:    nil,
		MilCh:     nil,
		Pre:       nil,
		Next:      nil,
		ValueLink: nil,
	}
}

type LinkNode struct {
	//Key   *[]byte
	Pre   *LinkNode
	Next  *LinkNode
	Value interface{}
}

func initLinkNode() *LinkNode {
	return &LinkNode{
		//Key:   nil,
		Pre:   nil,
		Next:  nil,
		Value: nil,
	}
}

type Tree struct {
	Root   *IndexNode
	Length uint64
	Depth  uint64
	Order  uint8
	Start  *LinkNode
	End    *LinkNode
}

func NewTree() *Tree {
	valueNode := initLinkNode()
	return &Tree{
		Root:   initNode(),
		Length: 0,
		Depth:  0,
		Order:  16,
		Start:  valueNode,
		End:    valueNode,
	}
}

func (t Tree) InsertOrUpdate(key []byte, value interface{}) error {
	//node := t.Root
	//keyLen := len(key)
	//for index := 0; index < keyLen; index++ {
	//	keyChat := key[index]
	//	indexKey := keyChat / 16
	//
	//	// init child node links
	//	if node.ChildPoints == nil {
	//		node.ChildPoints = initNode()
	//		node.ChildPoints.Parent = node
	//		node.ChildPoints.KeyChat = t.Order / 2
	//	}
	//
	//	//find match keyChat in ChildPoints -- index
	//	lastIndex := node.ChildPoints
	//	indexItem := node.ChildPoints
	//	for {
	//		if indexItem.KeyChat == indexKey {
	//			break
	//		}
	//
	//		if indexItem.KeyChat < indexKey {
	//			for indexItem != nil && indexItem.KeyChat <= indexKey {
	//				lastIndex = indexItem
	//				indexItem = indexItem.Next
	//			}
	//			break
	//		}
	//
	//		if indexItem.KeyChat > indexKey {
	//			for indexItem != nil && indexItem.KeyChat >= indexKey {
	//				lastIndex = indexItem
	//				indexItem = indexItem.Pre
	//			}
	//			break
	//		}
	//	}
	//
	//	if lastIndex.KeyChat != indexKey {
	//		newindex := initNode()
	//		newindex.KeyChat = indexKey
	//		if lastIndex.KeyChat < indexKey {
	//			newindex.Pre = lastIndex
	//			newindex.Next = lastIndex.Next
	//			lastIndex.Next = newindex
	//		} else {
	//			newindex.Pre = lastIndex.Pre
	//			newindex.Next = lastIndex
	//			lastIndex.Pre = newindex
	//		}
	//		lastIndex = newindex
	//	}
	//
	//	if lastIndex.ChildPoints == nil {
	//		lastIndex.ChildPoints = initNode()
	//		lastIndex.ChildPoints.Parent = node
	//		lastIndex.ChildPoints.KeyChat = keyChat
	//	}
	//
	//	lastmatch := lastIndex.ChildPoints
	//	linkItem := lastIndex.ChildPoints
	//	for linkItem != nil && linkItem.KeyChat <= keyChat {
	//		lastmatch = linkItem
	//		linkItem = linkItem.Next
	//	}
	//
	//	if lastmatch.KeyChat != keyChat {
	//		newnode := initNode()
	//		newnode.KeyChat = keyChat
	//		if lastmatch.KeyChat > newnode.KeyChat {
	//			newnode.Pre = lastmatch.Pre
	//			newnode.Next = lastmatch
	//			lastmatch.Pre = newnode
	//		}
	//		if lastmatch.KeyChat < newnode.KeyChat {
	//			newnode.Next = lastmatch.Next
	//			newnode.Pre = lastmatch
	//			lastmatch.Next = newnode
	//		}
	//
	//		lastmatch = newnode
	//	}
	//
	//	node = lastmatch
	//
	//	//last key chart save the value
	//	if index == keyLen-1 {
	//		node.KeyChat = keyChat
	//		node.Value = value
	//		break
	//	}
	//}
	return nil
}

func (t Tree) Get(key []byte) error {
	return nil
}

func (t Tree) Find(key []byte) error {
	return nil
}

func (t Tree) Delete(key []byte) error {
	return nil
}

func (t Tree) findChIdexNode(ch byte) *IndexNode {
	return nil
}

func (t Tree) findNode(key []byte) (*IndexNode, *LinkNode, error) {
	return nil, nil, nil
}

package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct { 
	Links []Link
	Data []byte 
}

func dfs(
	depth int,
	file File, store KVStore,
	seedIndex int,
	hasher hash.Hash
) (*Object, int) {

	if depth == 1 {
		
		if (len(file.Bytes()) - seedIndex) <= 256*1024 {
			
			data := file.Bytes()[seedIndex:]
			blob := Object{Links: nil, Data:  data,}
			jsonData, _ := json.Marshal(blob)
			hasher.Reset()
			hasher.Write(jsonData)
			exists, _ := store.Has(hasher.Sum(nil))
			
			if !exists {store.Put(hasher.Sum(nil), data)}
			
			return &blob, len(data)

		}
		
		links := &Object{}
		totalDataLength := 0
		
		for i := 1; i <= 4096; i++ {
			
			end := seedIndex + 256*1024

			if len(file.Bytes()) < end {end = len(file.Bytes())}

			data := file.Bytes()[seedIndex:end]

			blob := Object{Links: nil,Data:  data,}
			totalDataLength += len(data)
			jsonData, _ := json.Marshal(blob)
			hasher.Reset()
			hasher.Write(jsonData)
			exists, _ := store.Has(hasher.Sum(nil))

			if !exists {store.Put(hasher.Sum(nil), data)}

			links.Links = append(links.Links, Link{Hash: hasher.Sum(nil),Size: len(data),})
			links.Data = append(links.Data, []byte("blob")...)
			seedIndex += 256 * 1024

			if seedIndex >= len(file.Bytes()) {break}

		}

		jsonData, _ := json.Marshal(links)
		hasher.Reset()
		hasher.Write(jsonData)
		exists, _ = store.Has(hasher.Sum(nil))
		if !exists {store.Put(hasher.Sum(nil), jsonData)}
		return links, totalDataLength

	} else {

		links := &Object{}
		totalDataLength := 0

		for i := 1; i <= 4096; i++ {

			if seedIndex >= len(file.Bytes()) {break}
			tmp, length := dfsForSliceFile(depth-1, file, store, seedIndex, hasher)
			totalDataLength += length
			jsonData, _ := json.Marshal(tmp)
			hasher.Reset()
			hasher.Write(jsonData)
			links.Links = append(links.Links, Link{Hash: hasher.Sum(nil),Size: length,})
			typeName := "link"
			if tmp.Links == nil {typeName = "blob"}
			links.Data = append(links.Data, []byte(typeName)...)

		}

		jsonData, _ := json.Marshal(links)
		hasher.Reset()
		hasher.Write(jsonData)
		exists, _ := store.Has(hasher.Sum(nil))
		if !exists {store.Put(hasher.Sum(nil), jsonData)}
		return links, totalDataLength

	}
}

func sliceFile(
	file File, 
	store KVStore, 
	hasher hash.Hash
) *Object {

	if len(file.Bytes()) <= 256*1024 {

		data := file.Bytes()
		blob := Object{Links: nil,Data:  data,}
		jsonData, _ := json.Marshal(blob)
		hasher.Reset()
		hasher.Write(jsonData)
		exists, _ := store.Has(hasher.Sum(nil))
		if !exists {store.Put(hasher.Sum(nil), data)}
		return &blob

	}

	linkLength := (len(file.Bytes()) + (256*1024 - 1)) / (256 * 1024)
	depth := 0
	tmp := linkLength

	for {

		depth++
		tmp /= 4096
		if tmp == 0 {break}

	}

	result, _ := dfs(depth, file, store, 0, hasher)
	return result

}

func sliceDir(
	directory Dir, 
	store KVStore, 
	hasher hash.Hash
) *Object {

	iter := directory.It()
	treeObject := &Object{}

	for iter.Next() {

		node := iter.Node()

		if node.Type() == FILE {

			file := node.(File)
			tmp := sliceFile(file, store, hasher)
			jsonData, _ := json.Marshal(tmp)
			hasher.Reset()
			hasher.Write(jsonData)
			treeObject.Links = append(treeObject.Links, Link{Hash: hasher.Sum(nil),Size: int(file.Size()),Name: file.Name(),})
			typeName := "link"
			if tmp.Links == nil {typeName = "blob"}
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)

		} else {

			dir := node.(Dir)
			tmp := sliceDir(dir, store, hasher)
			jsonData, _ := json.Marshal(tmp)
			hasher.Reset()
			hasher.Write(jsonData)
			treeObject.Links = append(treeObject.Links, Link{Hash: hasher.Sum(nil),Size: int(dir.Size()),Name: dir.Name(),})
			typeName := "tree"
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)

		}
	}

	jsonData, _ := json.Marshal(treeObject)
	hasher.Reset()
	hasher.Write(jsonData)
	exists, _ := store.Has(hasher.Sum(nil))
	if !exists {store.Put(hasher.Sum(nil), jsonData)}
	return treeObject

}

func Add(store KVStore, 
	 node Node, 
	 hasher hash.Hash
	) []byte {

	if node.Type() == FILE {

		file := node.(File)
		tmp := sliceFile(file, store, hasher)
		jsonData, _ := json.Marshal(tmp)
		hasher.Write(jsonData)
		return hasher.Sum(nil)

	} else {

		directory := node.(Dir)
		tmp := sliceDir(directory, store, hasher)
		jsonData, _ := json.Marshal(tmp)
		hasher.Write(jsonData)
		return hasher.Sum(nil)
		
	}
}

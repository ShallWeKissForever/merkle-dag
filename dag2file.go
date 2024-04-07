package merkledag

import (
	"encoding/json"
	"strings"
)

const ChunkSize = 4

func GetFileByHashAndPath(store KVStore, hash []byte, path string, hp HashPool) []byte {

  exists, _ := store.Has(hash)
  
	if exists {
    
		objBinary, _ := store.Get(hash)
		obj := decodeObjectFromBinary(objBinary)
		pathSegments := strings.Split(path, "/")
		curIndex := 1
    
		return getFileFromDirectory(obj, pathSegments, curIndex, store)
    
	}
  
	return nil
  
}

func getFileFromDirectory(obj *Object, pathSegments []string, curIndex int, store KVStore) []byte {
  
	if curIndex >= len(pathSegments) {
		return nil
	}
  
	index := 0
  
	for i := range obj.Links {
    
		objType := string(obj.Data[index : index+ChunkSize])
		index += ChunkSize
		objInfo := obj.Links[i]
    
		if objInfo.Name != pathSegments[curIndex] {
			continue
		}
    
		switch objType {
      
		case TypeTree:
			objDirBinary, _ := store.Get(objInfo.Hash)
			objDir := decodeObjectFromBinary(objDirBinary)
			ans := getFileFromDirectory(objDir, pathSegments, curIndex+1, store)
			if ans != nil {
				return ans
			}
		case TypeBlob:
			ans, _ := store.Get(objInfo.Hash)
			return ans
		case TypeList:
			objLinkBinary, _ := store.Get(objInfo.Hash)
			objList := decodeObjectFromBinary(objLinkBinary)
			ans := getFileFromList(objList, store)
			return ans
      
		}
    
	}
  
	return nil
  
}

func getFileFromList(obj *Object, store KVStore) []byte {
  
	result := make([]byte, 0)
	index := 0
  
	for i := range obj.Links {
    
		curObjType := string(obj.Data[index : index+ChunkSize])
		index += ChunkSize
		curObjLink := obj.Links[i]
		curObjBinary, _ := store.Get(curObjLink.Hash)
		curObj := decodeObjectFromBinary(curObjBinary)
    
		if curObjType == TypeBlob {
			result = append(result, curObjBinary...)
		} else { // TypeList
			tmp := getFileFromList(curObj, store)
			result = append(result, tmp...)
		}
    
	}
  
	return result
  
}

func decodeObjectFromBinary(objBinary []byte) *Object {
  
	var obj Object
	json.Unmarshal(objBinary, &obj)
	return &obj
  
}

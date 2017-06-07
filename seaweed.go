package goseaweed

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"
)

func init() {

}

type Seaweed struct {
	Master    string
	HC        *HttpClient
	ChunkSize int64
	vc        VidCache // caching of volume locations, re-check if after 10 minutes
}

func NewSeaweed(master string) (sw *Seaweed) {
	return &Seaweed{
		Master: master,
		HC:     NewHttpClient(512, 45*time.Second),
	}
}

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     uint64 `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (sw *Seaweed) Assign(count int, collection string, ttl string) (*AssignResult, error) {
	values := make(url.Values)
	values.Set("count", strconv.Itoa(count))

	if collection != "" {
		values.Set("collection", collection)
	}
	if ttl != "" {
		values.Set("ttl", ttl)
	}
	jsonBlob, err := sw.HC.Post(sw.Master, "/dir/assign", values)
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func (sw *Seaweed) UploadFile(filePath, collection, ttl string) (ret *SubmitResult, err error) {
	fp, e := NewFilePart(filePath)
	if e != nil {
		return nil, e
	}
	fp.Collection = collection
	fp.Ttl = ttl
	ret, err = sw.UploadFilePart(&fp)
	if nil == err {
		fmt.Printf(" [x] %d bytes Uploaded %s\n", ret.Size, filePath)
	}
	return
}

func (sw *Seaweed) UploadViaReader(reader io.Reader, size int64, filename, collection, ttl string) (ret *SubmitResult, err error) {
	fp, e := NewFilePartFromReader(reader, size, filename)
	if e != nil {
		return nil, e
	}
	fp.Collection = collection
	fp.Ttl = ttl
	ret, err = sw.UploadFilePart(&fp)
	if nil == err {
		fmt.Printf(" [x] %d bytes Uploaded %s %s\n", ret.Size, filename, ret.Fid)
	}
	return
}

func (sw *Seaweed) UploadViaReaderWithFid(reader io.Reader, size int64, filename, server, fid string) (ret *SubmitResult, err error) {
	fp, e := NewFilePartFromReader(reader, size, filename)
	if e != nil {
		return nil, e
	}
	fp.Server, fp.Fid = server, fid

	ret, err = sw.UploadFilePart(&fp)
	if nil == err {
		fmt.Printf(" [x] %d bytes Uploaded %s\n", ret.Size, filename)
	}
	return
}

//Get a url for the file with fid
func (sw *Seaweed) WeedUrl(fid string) string {
	return fmt.Sprintf("http://%s/%s", sw.Master, fid)
}

func (sw *Seaweed) DownloadFileByFid(fid, dir string) (url, file string, n int64, err error) {
	url = fmt.Sprintf("http://%s/%s", sw.Master, fid)
	file, n, err = sw.DownloadFile(url, dir)
	return
}

func (sw *Seaweed) DownloadFile(url, dir string) (file string, n int64, err error) {
	filename, rc, err := sw.HC.DownloadUrl(url)
	if err != nil {
		return
	}

	// Ensure parent directory
	err = os.MkdirAll(dir, 0777)
	if nil != err {
		return
	}

	if filename == "" {
		filename = filepath.Base(url)
	}

	// Touch file
	file = path.Join(dir, filename)
	output, err := os.Create(file)
	if err != nil {
		return
	}
	defer output.Close()

	n, err = io.Copy(output, rc)
	if err != nil {
		return
	}

	fmt.Printf(" [x] %d bytes Downloaded %s\n", n, file)

	return
}

func (sw *Seaweed) BatchUploadFiles(files []string, collection string, ttl string) ([]SubmitResult, error) {

	fps, e := NewFileParts(files)
	if e != nil {
		return nil, e
	}
	return sw.BatchUploadFileParts(fps, collection, ttl)
}

func (sw *Seaweed) ReplaceFile(fid, filePath string, deleteFirst bool) error {
	fp, e := NewFilePart(filePath)
	if e != nil {
		return e
	}
	fp.Fid = fid
	_, e = sw.ReplaceFilePart(&fp, deleteFirst)
	return e
}

func (sw *Seaweed) DeleteFile(fileId, collection string) error {
	fileUrl, err := sw.LookupFileId(fileId, collection, false)
	if err != nil {
		return fmt.Errorf("Failed to lookup %s:%v", fileId, err)
	}
	err = sw.HC.Delete(fileUrl)
	if err != nil {
		return fmt.Errorf("Failed to delete %s:%v", fileUrl, err)
	}
	return nil
}

package goseaweed

import (
	"bytes"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type FilePart struct {
	Reader     io.Reader
	FileName   string
	FileSize   int64
	IsGzipped  bool
	MimeType   string
	Ext        string
	ModTime    int64 //in seconds
	Collection string
	Ttl        string
	Server     string
	Fid        string
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileBase string `json:"fileBase,omitempty"`
	FileUrl  string `json:"fileUrl,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
	MimeType string `json:"mime,omitempty"`
	Ext      string `json:"ext,omitempty"`
}

func (sw *Seaweed) BatchUploadFileParts(files []FilePart,
	collection string, ttl string) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
		results[index].FileBase = filepath.Base(file.FileName)
	}
	ret, err := sw.Assign(len(files), collection, ttl)
	if err != nil {
		for index := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}
	for index, file := range files {
		file.Fid = ret.Fid
		if index > 0 {
			file.Fid = file.Fid + "_" + strconv.Itoa(index)
		}
		file.Server = ret.PublicUrl /* ret.Url */ // TODO - revisit; Note - uploading to PublicUrl for Dev; must shift to Url for production!
		file.Collection = collection
		_, err = sw.UploadFilePart(&file)
		results[index].Size = file.FileSize
		if err != nil {
			results[index].Error = err.Error()
		}
		results[index].Fid = file.Fid + file.Ext
		results[index].FileUrl = ret.PublicUrl + "/" + file.Fid + file.Ext

		results[index].MimeType = file.MimeType
		results[index].Ext = file.Ext
	}
	return results, nil
}

func (sw *Seaweed) UploadFilePart(fp *FilePart) (ret *SubmitResult, err error) {
	if fp.Fid == "" {
		ret, err := sw.Assign(1, fp.Collection, fp.Ttl)
		if err != nil {
			return nil, err
		}
		fp.Server, fp.Fid = ret.PublicUrl /* ret.Url */, ret.Fid // TODO - revisit; Note - uploading to PublicUrl for Dev; must shift to Url for production!
	}
	if fp.Server == "" {
		if fp.Server, err = sw.LookupFileId(fp.Fid, fp.Collection, false); err != nil {
			return
		}
	}

	if closer, ok := fp.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	baseName := path.Base(fp.FileName)
	if sw.ChunkSize > 0 && fp.FileSize > sw.ChunkSize {
		chunks := fp.FileSize/sw.ChunkSize + 1
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fp.FileSize,
			Mime:   fp.MimeType,
			Chunks: make([]*ChunkInfo, 0, chunks),
		}

		for i := int64(0); i < chunks; i++ {
			id, count, e := sw.uploadChunk(fp, baseName+"-"+strconv.FormatInt(i+1, 10))
			if e != nil {
				// delete all uploaded chunks
				sw.DeleteChunks(&cm, fp.Collection)
				return nil, e
			}
			cm.Chunks = append(cm.Chunks,
				&ChunkInfo{
					Offset: i * sw.ChunkSize,
					Size:   int64(count),
					Fid:    id,
				},
			)
		}
		err = sw.uploadManifest(fp, &cm)
		if err != nil {
			// delete all uploaded chunks
			sw.DeleteChunks(&cm, fp.Collection)
		}
	} else {
		args := url.Values{}
		if fp.ModTime != 0 {
			args.Set("ts", strconv.FormatInt(fp.ModTime, 10))
		}
		fileUrl := MkUrl(fp.Server, fp.Fid, args)
		_, err = sw.HC.Upload(fileUrl, baseName, fp.Reader, fp.IsGzipped, fp.MimeType)
	}

	ret = &SubmitResult{}

	if err != nil {
		ret.Error = err.Error()
	} else {
		ret.MimeType = fp.MimeType
		ret.Ext = fp.Ext
		ret.Fid = fp.Fid + ret.Ext
		ret.Size = fp.FileSize
		ret.FileUrl = fp.Server + "/" + fp.Fid + ret.Ext
	}
	return
}

func (sw *Seaweed) ReplaceFilePart(fp *FilePart, deleteFirst bool) (ret *SubmitResult, err error) {
	if deleteFirst && fp.Fid != "" {
		sw.DeleteFile(fp.Fid, fp.Collection)
	}
	return sw.UploadFilePart(fp)
}

func (sw *Seaweed) uploadChunk(fp *FilePart, filename string) (fid string, size int64, e error) {
	ret, err := sw.Assign(1, fp.Collection, fp.Ttl)
	if err != nil {
		return "", 0, err
	}

	fileUrl, fid := MkUrl(ret.Url, ret.Fid, nil), ret.Fid
	reader := io.LimitReader(fp.Reader, sw.ChunkSize)
	uploadResult, uploadError := sw.HC.Upload(fileUrl, filename, reader, false, "application/octet-stream")
	if uploadError != nil {
		return fid, 0, uploadError
	}
	return fid, uploadResult.Size, nil
}

func (sw *Seaweed) uploadManifest(fp *FilePart, manifest *ChunkManifest) error {
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	bufReader := bytes.NewReader(buf)
	args := url.Values{}
	if fp.ModTime != 0 {
		args.Set("ts", strconv.FormatInt(fp.ModTime, 10))
	}
	args.Set("cm", "true")
	u := MkUrl(fp.Server, fp.Fid, args)
	_, e = sw.HC.Upload(u, manifest.Name, bufReader, false, "application/json")
	return e
}

func NewFilePartFromString(source, filename string) (ret FilePart, err error) {
	ret.Reader = strings.NewReader(source)
	ret.ModTime = time.Now().UnixNano() / int64(time.Millisecond)
	ret.FileName = filename

	ext := strings.ToLower(path.Ext(filename))
	ret.Ext = ext

	if ret.MimeType == "" && ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	size, err := strconv.ParseInt(strconv.Itoa(len(source)), 10, 64)
	ret.FileSize = size
	return
}

func NewFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		return ret, openErr
	}
	ret.Reader = fh

	if fi, fiErr := fh.Stat(); fiErr != nil {
		return ret, fiErr
	} else {
		ret.ModTime = fi.ModTime().UTC().Unix()
		ret.FileSize = fi.Size()
	}
	ext := strings.ToLower(path.Ext(fullPathFilename))
	ret.IsGzipped = ext == ".gz"
	if ret.IsGzipped {
		ret.FileName = fullPathFilename[0 : len(fullPathFilename)-3]
	}
	ret.FileName = fullPathFilename
	ret.Ext = ext

	if ret.MimeType == "" && ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

func NewFileParts(fullPathFilenames []string) (ret []FilePart, err error) {
	ret = make([]FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = NewFilePart(file); err != nil {
			return
		}
	}
	return
}

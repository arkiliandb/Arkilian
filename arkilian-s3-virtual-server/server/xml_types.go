package server

import "encoding/xml"

// ListBucketResult defines S3 GET Bucket (List Objects) XML response.
type ListBucketResult struct {
	XMLName        xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name           string   `xml:"Name"`
	Prefix         string   `xml:"Prefix"`
	Marker         string   `xml:"Marker"`
	MaxKeys        int      `xml:"MaxKeys"`
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []Object `xml:"Contents"`
	CommonPrefixes []Prefix `xml:"CommonPrefixes,omitempty"`
}

// Object represents an S3 object entry in ListBucketResult.
type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// Prefix represents a common prefix for directory-style listing.
type Prefix struct {
	Prefix string `xml:"Prefix"`
}

// S3Error represents an S3 XML error response.
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestId string   `xml:"RequestId"`
	HostId    string   `xml:"HostId"`
}

// PresignedURLRequest represents the request payload for /v1/upload/request.
type PresignedURLRequest struct {
	DBID     string `json:"db_id"`
	FileName string `json:"file_name"`
	Type     string `json:"type"`
}

// PresignedURLResponse represents the response for /v1/upload/request.
type PresignedURLResponse struct {
	UploadURL string `json:"upload_url"`
	S3Key     string `json:"s3_key"`
	ExpiresIn int    `json:"expires_in"`
}

// WALPushPayload represents incoming CDC streaming writes from Arkilian clients.
type WALPushPayload struct {
	DBID      string        `json:"db_id"`
	PayloadID int64         `json:"payload_id"`
	SQL       string        `json:"sql"`
	Params    []interface{} `json:"params"`
}

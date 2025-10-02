package oss

import (
	"bytes"
	"fmt"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/rs/zerolog/log"
)

type OSSCOnfig struct {
	Endpoint        string
	AccessKeyId     string
	AccessKeySecret string
	Bucket          string
	// URL tidak lagi digunakan untuk akses publik langsung,
	// tapi bisa disimpan untuk referensi atau bagian dari base URL.
	Url string
}

type OSSInterface interface {
	Start() (client *oss.Client, err error)
	// Upload sekarang tidak mengembalikan URL publik.
	Upload(key string, object []byte) (err error)
	// Fungsi baru untuk mendapatkan URL sementara yang bisa diakses.
	GetSignedURL(key string, expiredInSec int64) (signedURL string, err error)
}

type ossInstance struct {
	*OSSCOnfig
	client *oss.Client
}

func NewClient(conf *OSSCOnfig) OSSInterface {
	return &ossInstance{
		conf,
		nil,
	}
}

func (o *ossInstance) Start() (client *oss.Client, err error) {
	client, err = oss.New(o.Endpoint, o.AccessKeyId, o.AccessKeySecret)
	if err != nil {
		log.Error().Err(err).Msg("error on starting oss client")
		return
	}

	// create bucket
	exist, err := client.IsBucketExist(o.Bucket)
	if err != nil {
		log.Error().Err(err).Msg("error on check bucket")
		return
	}
	if !exist {
		err = client.CreateBucket(o.Bucket)
		if err != nil {
			log.Error().Err(err).Msg("error on create bucket")
			return
		}
	}

	o.client = client
	return
}

// Mengubah fungsi Upload untuk menggunakan ACL Private.
// Fungsi ini tidak lagi mengembalikan URL karena objeknya bersifat private.
func (o *ossInstance) Upload(key string, object []byte) (err error) {
	bucket, err := o.client.Bucket(o.Bucket)
	if err != nil {
		log.Error().Err(err).Msg("error on access bucket")
		return
	}

	// Mengganti ACL menjadi oss.ACLPrivate
	err = bucket.PutObject(key, bytes.NewReader(object), oss.ObjectACL(oss.ACLPrivate))
	if err != nil {
		log.Error().Err(err).Msg("error on put file to bucket")
		return
	}
	// URL tidak lagi dibuat karena objeknya private dan tidak bisa diakses langsung.
	return
}

// Fungsi baru untuk mendapatkan URL yang ditandatangani (signed URL) untuk objek private.
// URL ini memiliki waktu kedaluwarsa.
func (o *ossInstance) GetSignedURL(key string, expiredInSec int64) (signedURL string, err error) {
	bucket, err := o.client.Bucket(o.Bucket)
	if err != nil {
		log.Error().Err(err).Msg("error on access bucket")
		return
	}

	// Membuat signed URL yang valid untuk metode GET selama waktu yang ditentukan.
	signedURL, err = bucket.SignURL(key, oss.HTTPGet, expiredInSec)
	if err != nil {
		log.Error().Err(err).Msg("error on generating signed url")
		return
	}

	return
}

package gobatsd

import (
	"crypto/md5"
	"encoding/hex"
	"io"
)

func CalculateFilename(metric string, root string) string {

	h := md5.New()
	io.WriteString(h, metric)
	metricHash := hex.EncodeToString(h.Sum([]byte{}))
	return root + "/" + metricHash[0:2] + "/" + metricHash[2:4] + "/" + metricHash
}

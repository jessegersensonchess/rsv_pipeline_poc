package main

// Bitmap struct to hold the bitmap data.
type Bitmap struct {
	data []uint64
}

// NewBitmap allocates a bitmap that can store bits for IDs [0, maxID].
func NewBitmap(maxID int) *Bitmap {
	nWords := (maxID + 64) / 64
	return &Bitmap{
		data: make([]uint64, nWords),
	}
}

func (bm *Bitmap) Add(id int) {
	if id < 0 {
		return
	}
	word := id / 64
	if word < 0 || word >= len(bm.data) {
		// ID is out of range for this bitmap; either ignore or log
		// fmt.Printf("ID %d is out of range for bitmap\n", id)
		return
	}
	bit := uint(id % 64)
	bm.data[word] |= 1 << bit
}

//// Add adds an ID to the bitmap.
//func (bm *Bitmap) Add(id int) {
//	if id < 0 {
//		return
//	}
//	word := id / 64
//	bit := uint(id % 64)
//	bm.data[word] |= 1 << bit
//}

// Has checks if an ID is in the bitmap.
func (bm *Bitmap) Has(id int) bool {
	if id < 0 {
		return false
	}
	word := id / 64
	if word < 0 || word >= len(bm.data) {
		return false
	}
	bit := uint(id % 64)
	return (bm.data[word] & (1 << bit)) != 0
}

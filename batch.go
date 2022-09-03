// Copyright (C) 2022  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"

	"github.com/tsuna/gohbase/hrpc"
)

// Batch is used to contain multiple HBASE mutate calls. Create a
// batch with [NewBatch] and pass to [SendBatch] to apply the batch to
// HBASE.
type Batch struct {
	table []byte
	ms    []*hrpc.Mutate
}

// NewBatch instantiates a Batch with a table and a slice of Mutates.
func NewBatch(table []byte, ms []*hrpc.Mutate) *Batch {
	b := &Batch{
		table: table,
		ms:    ms,
	}
	for _, m := range b.ms {
		b.checkCall(m)
	}
	return b
}

// Add adds a Call to this Batch. Do not call Add after passing the
// batch to SendBatch. This could cause a data race.
//
// Do we need this?
func (b *Batch) Add(m *hrpc.Mutate) {
	b.checkCall(m)
	b.ms = append(b.ms, m)
}

func (b *Batch) checkCall(m *hrpc.Mutate) {
	table := m.Table()
	if !bytes.Equal(table, b.table) {
		panic("Call's table should the same as the batch's table")
	}
	if m.SkipBatch() {
		panic("Call is not batchable")
	}
}

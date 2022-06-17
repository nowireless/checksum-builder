package main

import (
	"context"
	"crypto/sha256"
	"go.uber.org/zap"
	"io"
	"io/fs"
	"os"
	"sync"
)

type WorkUnit struct {
	Path string
	Info fs.FileInfo
}

type WorkResult struct {
	Checksum []byte
	WorkUnit WorkUnit
	// The following fields are from the work unit
	//Path      string
	//FileMode  os.FileMode
	//SizeBytes int64
}

type Worker struct {
	id     int
	logger *zap.Logger

	workQueue   chan WorkUnit
	resultQueue chan WorkResult
	ctx         context.Context

	wg *sync.WaitGroup
}

func (w *Worker) Start() {
	defer w.wg.Done()
	logger := w.logger
	logger.Info("Starting worker")

	for {
		select {
		case workUnit := <-w.workQueue:
			logger.Debug("Received work unit", zap.Any("workUnit", workUnit))
			w.Process(workUnit)
		case <-w.ctx.Done():
			logger.Info("Worker finished")
			return
		}
	}
}

func (w *Worker) Process(workUnit WorkUnit) {
	logger := w.logger

	// TODO put these into a file or something
	//fileMode := workUnit.Info.Mode()
	//sizeBytes := workUnit.Info.Size()
	isDirectory := workUnit.Info.IsDir()

	if isDirectory {
		// TODO something with this info????
	} else {
		// Calculate checksum
		f, err := os.Open(workUnit.Path)
		if err != nil {
			logger.Error("Failed to open file", zap.Any("workUnit", workUnit), zap.Error(err))
			return
		}
		defer f.Close()

		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			logger.Error("Failed to checksum file", zap.Any("workUnit", workUnit), zap.Error(err))
		}

		w.resultQueue <- WorkResult{
			Checksum: h.Sum(nil),
			WorkUnit: workUnit,
		}
	}
}

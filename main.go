package main

import (
	"context"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	logLevel := zap.NewAtomicLevelAt(zap.InfoLevel)

	encoderCfg := zap.NewProductionEncoderConfig()
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		logLevel,
	))

	if len(os.Args) != 2 {
		logger.Fatal("Missing required argument")
	}

	rootPath := os.Args[1]

	workQueue := make(chan WorkUnit)
	workResultQueue := make(chan WorkResult)

	//
	// Create Workers
	//
	var workerWg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(context.Background())

	for id := 0; id < 32; id++ {
		workerWg.Add(1)

		worker := Worker{
			id:          id,
			logger:      logger.With(zap.Int("WorkerID", id)),
			workQueue:   workQueue,
			resultQueue: workResultQueue,
			ctx:         workerCtx,
			wg:          &workerWg,
		}

		go worker.Start()
	}

	//
	// Create Result processors
	//
	var resultWg sync.WaitGroup
	resultCtx, resultCancel := context.WithCancel(context.Background())
	//for id := 0; id < 1; id++ { // For right now let there be 1 result processor
	resultWg.Add(1)

	id := 0
	processor := ResultProcessorRunner{
		id:          id,
		logger:      logger.With(zap.Int("ResultProcessorID", id)),
		resultQueue: workResultQueue,
		ctx:         resultCtx,
		wg:          &resultWg,

		resultProcessor: NewJsonLineResultProcessor(logger, "output_results.jsonl"),
	}

	go processor.Start()
	//}

	// Create work for the workers!
	err := filepath.Walk(rootPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			logger.Error("Encountered error with filepath.WalkFunc", zap.Error(err))
			return err
		}

		wu := WorkUnit{
			Path: path,
			Info: info,
		}

		logger.Debug("Preparing to submit work unit", zap.Any("workUnit", wu))

		workQueue <- wu

		return nil
	})

	if err != nil {
		logger.Fatal("Failed to walk directory", zap.Error(err))
	}

	time.Sleep(10 * time.Second)

	// No more work, stop the workers
	workerCancel()
	workerWg.Wait()
	logger.Info("All workers completed")

	// Now stop the result processors
	resultCancel()
	resultWg.Wait()
	logger.Info("All result processors completed")
}

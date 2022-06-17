package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/paulbellamy/ratecounter"
	"go.uber.org/zap"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// TODO Future design
// - The ResultProcessorRunner should remain a singleton
// - It should allow the ResultProcessor to be parallelize

type ResultProcessor interface {
	Process(logger *zap.Logger, result WorkResult)
	Close() error
}

type ResultProcessorRunner struct {
	id          int
	logger      *zap.Logger
	resultQueue chan WorkResult
	ctx         context.Context
	wg          *sync.WaitGroup

	fileProcessed *uint64

	resultProcessor ResultProcessor
}

func (p *ResultProcessorRunner) Start() {
	defer p.wg.Done()
	logger := p.logger
	logger.Info("Starting Result processor")

	resultCounter := ratecounter.NewRateCounter(time.Second)
	go p.startLogger(resultCounter)

	var fileProcessed uint64
	p.fileProcessed = &fileProcessed

	for {
		select {
		case workResult := <-p.resultQueue:
			logger.Debug("Received work unit", zap.Any("workResult", workResult))
			p.resultProcessor.Process(logger, workResult)
			resultCounter.Incr(1)
			atomic.AddUint64(p.fileProcessed, 1)
		case <-p.ctx.Done():
			p.resultProcessor.Close()
			logger.Info("Result processor finished")
			return
		}
	}
}

func (p *ResultProcessorRunner) startLogger(rateCounter *ratecounter.RateCounter) {
	logger := p.logger

	running := true
	for running {
		logger.Info("Statistics", zap.Uint64("files processed", atomic.LoadUint64(p.fileProcessed)), zap.Any("files/second", rateCounter.Rate()))
		select {
		case <-time.After(10 * time.Second):
		case <-p.ctx.Done():
			logger.Debug("Logger thread ending")
			running = false
		}
	}
}

func DummyResultProcessor(logger *zap.Logger, result WorkResult) {
	logger.Info("Received Work Result", zap.Any("result", result))
}

type JsonLineResultProcessor struct {
	outputFile *os.File
	encoder    *json.Encoder
}

func NewJsonLineResultProcessor(logger *zap.Logger, outputPath string) ResultProcessor {
	f, err := os.Create(outputPath)
	if err != nil {
		logger.Fatal("Failed to open results file", zap.Error(err), zap.String("outputPath", outputPath))
	}

	return &JsonLineResultProcessor{
		outputFile: f,
		encoder:    json.NewEncoder(f),
	}
}

func (jp *JsonLineResultProcessor) Close() error {
	return jp.outputFile.Close()
}

func (jp *JsonLineResultProcessor) Process(logger *zap.Logger, result WorkResult) {
	line := map[string]interface{}{}
	line["Path"] = result.WorkUnit.Path
	line["Checksum"] = fmt.Sprintf("%x", result.Checksum)
	line["SizeBytes"] = result.WorkUnit.Info.Size()

	err := jp.encoder.Encode(line)
	if err != nil {
		logger.Error("Failed to encode result line", zap.Error(err), zap.Any("line", line))
	}
}

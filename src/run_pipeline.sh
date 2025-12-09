#!/bin/bash

# 1. Define Colors and Paths
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color
DELTA_PKG="io.delta:delta-core_2.12:2.4.0"

echo -e "${GREEN}🚀 STARTING FINANCE LAKE PIPELINE...${NC}"

# ---------------------------------------------------------
# Stage 1: Data Collection (Streaming)
# ---------------------------------------------------------
echo -e "${YELLOW}🌊 [1/4] Starting Spark Streaming & Kafka (Collecting Data)...${NC}"
echo "   > Running for 60 seconds to generate Gold Data..."

# Launch Streaming in the background
spark-submit --packages $DELTA_PKG /app/src/processing/spark_streaming_main.py > /app/logs/streaming_job.log 2>&1 &
STREAMING_PID=$!

# Wait for 60 seconds to populate data (increase duration if more data is needed)
# Display progress bar
for i in {1..60}; do
    echo -ne "   ⏳ Collecting Data... $i/60s \r"
    sleep 1
done
echo ""

# ---------------------------------------------------------
# Stage 2: Free RAM - Critical Step
# ---------------------------------------------------------
echo -e "${YELLOW}🛑 [2/4] Stopping Streaming to free RAM for ML...${NC}"
kill $STREAMING_PID
sleep 5 # Brief pause for process cleanup
echo -e "${GREEN}   ✅ RAM Freed! Ready for ML.${NC}"

# ---------------------------------------------------------
# Stage 3: Machine Learning Execution
# ---------------------------------------------------------
echo -e "${YELLOW}🤖 [3/4] Running ML Training & Inference...${NC}"

# A. Model Training
echo "   > Training Model..."
spark-submit --packages $DELTA_PKG /app/src/ml/stock_predictor.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}   ✅ Model Trained Successfully.${NC}"
else
    echo -e "${RED}   ❌ Training Failed! Check logs.${NC}"
    exit 1
fi

# B. Inference / Prediction
echo "   > Running Inference..."
spark-submit --packages $DELTA_PKG /app/src/ml/stock_inference.py

# ---------------------------------------------------------
# Stage 4: Analysis and Reporting
# ---------------------------------------------------------
echo -e "${YELLOW}📊 [4/4] Generating Final Report...${NC}"
rm -rf /app/data/ml/model
spark-submit --packages $DELTA_PKG /app/src/ml/analyze_results.py

echo -e "${GREEN}🎉 PIPELINE FINISHED! Check /app/logs/analysis_report.txt${NC}"
tail -f /dev/null
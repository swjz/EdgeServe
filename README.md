```
pip install cvxpy
pip install cvxopt

# in four separate terminal windows
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
torchserve --start --ncs --model-store model_store --models densenet161.mar
python main.py
```

# Azuer, `azure_test`

```
python device_driver.py dev0
python device_driver.py dev1
python optimizer_driver.py
```

# Local, `master`

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

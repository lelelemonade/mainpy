import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
import uuid

profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(['47.100.88.11']),
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=tuple_factory
)

cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile}, contact_points=['127.0.0.1'], port=9042)
session = cluster.connect()
session.set_keyspace('cloud_factory')
session.execute('USE cloud_factory')

start_time = time.time()

for i in range(1000):
    device_id = str(uuid.uuid4())
    category_name = str(uuid.uuid4())
    firmware_id = str(uuid.uuid4())
    hardware_id = str(uuid.uuid4())
    hardware_type = str(uuid.uuid4())
    hardware_version = str(uuid.uuid4())
    mac = str(uuid.uuid4())
    model_id = str(i)
    production_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1610594353 + i))
    sn = str(uuid.uuid4())
    version = str(i)
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1610594353 + i))
    updated_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1610594353 + i))

    sql = 'INSERT INTO cloud_factory.device_factory_info (device_id, category_name, firmware_id, hardware_id, hardware_type, hardware_version, mac, model_id, production_date, sn, version, create_time, updated_time) VALUES ' \
          '(\'' + device_id + '\',' \
                              '\'' + category_name + '\',' \
                                                     '\'' + firmware_id + '\',' \
                                                                          '\'' + hardware_id + '\',' \
                                                                                               '\'' + hardware_type + '\',' \
                                                                                                                      '\'' + hardware_version + '\',' \
                                                                                                                                                '\'' + mac + '\',' \
                                                                                                                                                             '' + model_id + ',' \
                                                                                                                                                                             '\'' + production_date + '\',' \
                                                                                                                                                                                                      '\'' + sn + '\',' \
                                                                                                                                                                                                                  '' + version + ',' \
                                                                                                                                                                                                                                   '\'' + create_time + '\',' \
                                                                                                                                                                                                                                                        '\'' + updated_time + '\')'

    result = session.execute(sql)
    print(i)

end_time = time.time()

print("Hello")
print("totol time : %s seconds", end_time - start_time)

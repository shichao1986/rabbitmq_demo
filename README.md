# rabbitmq_demo
rabbitmq使用示例</p>
本例实现rabbitmq中的producer和consumer</p>
rabbitmq的配置采用config.py单独完成</p>
rabbit_utils.py封装了producer和consumer的单例模式</p>
核心类为ConsumerThread，该类负责维护和rabbitmq之间的connection</p>
channel被交由业务层去定义，配置，使用</p>
channel的独立使用保证了业务之间的独立特性（如回调处理，qos等的不同配置）</p>

## kafka learn


## 拦截器案例

- 1.需求
    
    实现一个简单的双interceptor组成的拦截链。第一个interceptor 会在消息发送前将时间戳加到消息value的最前部；第二个interceptor 会在消息发送后更新成功发送消息数或失败发送消息数。
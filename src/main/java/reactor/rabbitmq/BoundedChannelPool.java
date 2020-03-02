/*
 * Copyright (c) 2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.pool.PooledRef;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static reactor.rabbitmq.ChannelCloseHandlers.SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE;

/**
 * Default implementation of {@link ChannelPool}.
 * <p>
 * This channel pool is lazy initialized. Contrarly to {@link LazyChannelPool}
 * it is limited to {@link ChannelPoolOptions#getPoolSize()}.
 * It always tries to obtain a channel from the pool. However, in case of high-concurrency environments,
 * number of channels might exceed channel pool maximum size.
 * <p>
 * Channels are added to the pool after their use {@link ChannelPool#getChannelCloseHandler()}
 * and obtained from the pool when channel is requested {@link ChannelPool#getChannelMono()}.
 * <p>
 * If the pool is empty, a new channel is created.
 * If a channel is no longer needed and the channel pool is full, then the channel is being closed.
 * If a channel is no longer needed and the channel pool has not reached its
 * capacity, then the channel is added to the pool.
 * <p>
 *
 * @since 1.5.0
 */
class BoundedChannelPool implements ChannelPool {

    private static final int DEFAULT_CHANNEL_POOL_SIZE = 5;

    private final Scheduler subscriptionScheduler;
    private final InstrumentedPool<Channel> pool;

    BoundedChannelPool(Mono<? extends Connection> connectionMono, ChannelPoolOptions channelPoolOptions) {
        int poolSize = channelPoolOptions.getPoolSize() == null ?
                DEFAULT_CHANNEL_POOL_SIZE : channelPoolOptions.getPoolSize();
        this.pool = PoolBuilder
            .from(connectionMono.map(this::createChannel))
            .sizeBetween(1, poolSize)
            .destroyHandler(this::destroyConnection)
            .fifo();
        this.subscriptionScheduler = channelPoolOptions.getSubscriptionScheduler() == null ?
                Schedulers.newElastic("sender-channel-pool") : channelPoolOptions.getSubscriptionScheduler();
    }

    public Mono<? extends Channel> getChannelMono() {
        return pool.acquire()
            .doOnNext(i -> System.out.println("acquired " + i))
            .map(Wrapper::new)
            .subscribeOn(subscriptionScheduler);
    }

    @Override
    public BiConsumer<SignalType, Channel> getChannelCloseHandler() {
        return (signalType, channel) -> {
            if (channel instanceof Wrapper) {
                Wrapper c = (Wrapper) channel;
                System.out.println("close channel: " + c.isOpen());
                if (!c.isOpen()) {
                    c.invalidate();
                } else {
                    c.release();
                }
            }
        };
    }

    @Override
    public void close() {
        pool.dispose();
    }

    private Channel createChannel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new RabbitFluxException("Error while creating channel", e);
        }
    }

    private Publisher<Void> destroyConnection(Channel channel) {
        return Mono.fromRunnable(() -> {
            try {
                channel.close();
            } catch (Exception ignored) {
            }
        });
    }

    private class Wrapper implements Channel {
        private final PooledRef<Channel> ref;

        private Wrapper(PooledRef<Channel> ref) {
            this.ref = ref;
        }

        public void invalidate() {
            ref.invalidate().block();
        }

        public void release() {
            ref.release().block();
            System.out.println("released " + ref);
        }

        private Channel unwrap() {
            return ref.poolable();
        }

        @Override
        public int getChannelNumber() {
            return unwrap().getChannelNumber();
        }

        @Override
        public Connection getConnection() {
            return unwrap().getConnection();
        }

        @Override
        public void close() throws IOException, TimeoutException {
            unwrap().close();
        }

        @Override
        public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
            unwrap().close(closeCode, closeMessage);
        }

        @Override
        public void abort() throws IOException {
            unwrap().abort();
        }

        @Override
        public void abort(int closeCode, String closeMessage) throws IOException {
            unwrap().abort(closeCode, closeMessage);
        }

        @Override
        public void addReturnListener(ReturnListener listener) {
            unwrap().addReturnListener(listener);
        }

        @Override
        public ReturnListener addReturnListener(ReturnCallback returnCallback) {
            return unwrap().addReturnListener(returnCallback);
        }

        @Override
        public boolean removeReturnListener(ReturnListener listener) {
            return unwrap().removeReturnListener(listener);
        }

        @Override
        public void clearReturnListeners() {
            unwrap().clearReturnListeners();
        }

        @Override
        public void addConfirmListener(ConfirmListener listener) {
            unwrap().addConfirmListener(listener);
        }

        @Override
        public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
            return unwrap().addConfirmListener(ackCallback, nackCallback);
        }

        @Override
        public boolean removeConfirmListener(ConfirmListener listener) {
            return unwrap().removeConfirmListener(listener);
        }

        @Override
        public void clearConfirmListeners() {
            unwrap().clearConfirmListeners();
        }

        @Override
        public Consumer getDefaultConsumer() {
            return unwrap().getDefaultConsumer();
        }

        @Override
        public void setDefaultConsumer(Consumer consumer) {
            unwrap().setDefaultConsumer(consumer);
        }

        @Override
        public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
            unwrap().basicQos(prefetchSize, prefetchCount, global);
        }

        @Override
        public void basicQos(int prefetchCount, boolean global) throws IOException {
            unwrap().basicQos(prefetchCount, global);
        }

        @Override
        public void basicQos(int prefetchCount) throws IOException {
            unwrap().basicQos(prefetchCount);
        }

        @Override
        public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
            unwrap().basicPublish(exchange, routingKey, props, body);
        }

        @Override
        public void basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) throws IOException {
            unwrap().basicPublish(exchange, routingKey, mandatory, props, body);
        }

        @Override
        public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) throws IOException {
            unwrap().basicPublish(exchange, routingKey, mandatory, immediate, props, body);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
            return unwrap().exchangeDeclare(exchange, type);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
            return unwrap().exchangeDeclare(exchange, type);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable, autoDelete, arguments);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable, autoDelete, arguments);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
        }

        @Override
        public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            unwrap().exchangeDeclareNoWait(exchange, type, durable, autoDelete, internal, arguments);
        }

        @Override
        public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
            unwrap().exchangeDeclareNoWait(exchange, type, durable, autoDelete, internal, arguments);
        }

        @Override
        public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
            return unwrap().exchangeDeclarePassive(name);
        }

        @Override
        public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
            return unwrap().exchangeDelete(exchange, ifUnused);
        }

        @Override
        public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
            unwrap().exchangeDeleteNoWait(exchange, ifUnused);
        }

        @Override
        public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
            return unwrap().exchangeDelete(exchange);
        }

        @Override
        public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) throws IOException {
            return unwrap().exchangeBind(destination, source, routingKey);
        }

        @Override
        public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeBind(destination, source, routingKey, arguments);
        }

        @Override
        public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
            unwrap().exchangeBindNoWait(destination, source, routingKey, arguments);
        }

        @Override
        public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) throws IOException {
            return unwrap().exchangeUnbind(destination, source, routingKey);
        }

        @Override
        public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
            return unwrap().exchangeUnbind(destination, source, routingKey, arguments);
        }

        @Override
        public void exchangeUnbindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
            unwrap().exchangeUnbindNoWait(destination, source, routingKey, arguments);
        }

        @Override
        public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
            return unwrap().queueDeclare();
        }

        @Override
        public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            return unwrap().queueDeclare(queue, durable, exclusive, autoDelete, arguments);
        }

        @Override
        public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
            unwrap().queueDeclareNoWait(queue, durable, exclusive, autoDelete, arguments);
        }

        @Override
        public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
            return unwrap().queueDeclarePassive(queue);
        }

        @Override
        public AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
            return unwrap().queueDelete(queue);
        }

        @Override
        public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
            return unwrap().queueDelete(queue, ifUnused, ifEmpty);
        }

        @Override
        public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
            unwrap().queueDeleteNoWait(queue, ifUnused, ifEmpty);
        }

        @Override
        public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) throws IOException {
            return unwrap().queueBind(queue, exchange, routingKey);
        }

        @Override
        public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            return unwrap().queueBind(queue, exchange, routingKey, arguments);
        }

        @Override
        public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            unwrap().queueBindNoWait(queue, exchange, routingKey, arguments);
        }

        @Override
        public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) throws IOException {
            return unwrap().queueUnbind(queue, exchange, routingKey);
        }

        @Override
        public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
            return unwrap().queueUnbind(queue, exchange, routingKey, arguments);
        }

        @Override
        public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
            return unwrap().queuePurge(queue);
        }

        @Override
        public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
            return unwrap().basicGet(queue, autoAck);
        }

        @Override
        public void basicAck(long deliveryTag, boolean multiple) throws IOException {
            unwrap().basicAck(deliveryTag, multiple);
        }

        @Override
        public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
            unwrap().basicNack(deliveryTag, multiple, requeue);
        }

        @Override
        public void basicReject(long deliveryTag, boolean requeue) throws IOException {
            unwrap().basicReject(deliveryTag, requeue);
        }

        @Override
        public String basicConsume(String queue, Consumer callback) throws IOException {
            return unwrap().basicConsume(queue, callback);
        }

        @Override
        public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
            return unwrap().basicConsume(queue, deliverCallback, cancelCallback);
        }

        @Override
        public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, deliverCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, deliverCallback, cancelCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, callback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, deliverCallback, cancelCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, deliverCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, deliverCallback, cancelCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, arguments, callback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, arguments, deliverCallback, cancelCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, arguments, deliverCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, arguments, deliverCallback, cancelCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, callback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, deliverCallback, cancelCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, deliverCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, deliverCallback, cancelCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, shutdownSignalCallback);
        }

        @Override
        public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
            return unwrap().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback, shutdownSignalCallback);
        }

        @Override
        public void basicCancel(String consumerTag) throws IOException {
            unwrap().basicCancel(consumerTag);
        }

        @Override
        public AMQP.Basic.RecoverOk basicRecover() throws IOException {
            return unwrap().basicRecover();
        }

        @Override
        public AMQP.Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
            return unwrap().basicRecover(requeue);
        }

        @Override
        public AMQP.Tx.SelectOk txSelect() throws IOException {
            return unwrap().txSelect();
        }

        @Override
        public AMQP.Tx.CommitOk txCommit() throws IOException {
            return unwrap().txCommit();
        }

        @Override
        public AMQP.Tx.RollbackOk txRollback() throws IOException {
            return unwrap().txRollback();
        }

        @Override
        public AMQP.Confirm.SelectOk confirmSelect() throws IOException {
            return unwrap().confirmSelect();
        }

        @Override
        public long getNextPublishSeqNo() {
            return unwrap().getNextPublishSeqNo();
        }

        @Override
        public boolean waitForConfirms() throws InterruptedException {
            return unwrap().waitForConfirms();
        }

        @Override
        public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
            return unwrap().waitForConfirms(timeout);
        }

        @Override
        public void waitForConfirmsOrDie() throws IOException, InterruptedException {
            unwrap().waitForConfirmsOrDie();
        }

        @Override
        public void waitForConfirmsOrDie(long timeout) throws IOException, InterruptedException, TimeoutException {
            unwrap().waitForConfirmsOrDie(timeout);
        }

        @Override
        public void asyncRpc(Method method) throws IOException {
            unwrap().asyncRpc(method);
        }

        @Override
        public Command rpc(Method method) throws IOException {
            return unwrap().rpc(method);
        }

        @Override
        public long messageCount(String queue) throws IOException {
            return unwrap().messageCount(queue);
        }

        @Override
        public long consumerCount(String queue) throws IOException {
            return unwrap().consumerCount(queue);
        }

        @Override
        public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
            return unwrap().asyncCompletableRpc(method);
        }

        @Override
        public void addShutdownListener(ShutdownListener listener) {
            unwrap().addShutdownListener(listener);
        }

        @Override
        public void removeShutdownListener(ShutdownListener listener) {
            unwrap().removeShutdownListener(listener);
        }

        @Override
        public ShutdownSignalException getCloseReason() {
            return unwrap().getCloseReason();
        }

        @Override
        public void notifyListeners() {
            unwrap().notifyListeners();
        }

        @Override
        public boolean isOpen() {
            return unwrap().isOpen();
        }
    }
}

defmodule Kafkex do
  def connect({host, port}, timeout \\ 500) when is_integer(port) and is_integer(port) do
    options = [:binary, {:active, :false}, {:packet, :raw}]
    {:ok, socket} = :gen_tcp.connect(host, port, options, timeout)
    socket
  end

  def metadata(socket, topics, timeout, correlation_id \\ 0, client_id \\ "kafkaex") do
    request = encode_metadata_request(correlation_id, client_id, topics)
    :ok = :gen_tcp.send(socket, request)
    recv_metadata_response(socket, timeout)
  end

  def encode_metadata_request(correlation_id, client_id, topics) do
    api_key = 3
    api_version = 0
    {encoded_topics_size, encoded_topics} = encode_metadata_topics(topics)
    client_id_size = byte_size(client_id)
    request_size = 2 + 2 + 4 + 2 + client_id_size + encoded_topics_size
    [<<request_size::size(32), api_key::size(16), api_version::size(16),
      correlation_id::size(32), client_id_size::size(16)>>,
      client_id, encoded_topics]
  end

  def encode_metadata_topics(topics) do
    encode_metadata_topics(topics, 0, 0, [])
  end

  def encode_metadata_topics([topic | topics], count, size, encoded) when is_binary(topic) do
    topic_size = byte_size(topic)
    encode_metadata_topics(topics, count + 1, size + 2 + topic_size, [encoded, <<topic_size::size(16)>>, topic])
  end

  def encode_metadata_topics([], count, size, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  def decode_array32(<<>>, vals) do
    List.flatten(vals)
  end

  def decode_array32(<<val::size(32), rest::binary>>, vals) do
    decode_array32(rest, [vals, val])
  end

  def recv_metadata_response(socket, timeout) do
    {:ok, <<response_size::size(32)>>} = :gen_tcp.recv(socket, 4, timeout)
    {:ok, <<correlation_id::size(32), num_brokers::size(32)>>} = :gen_tcp.recv(socket, 8, timeout)
    brokers = Enum.map(1..num_brokers, fn(_) ->
      {:ok, <<node_id::size(32), host_size::size(16)>>} = :gen_tcp.recv(socket, 6, timeout)
      {:ok, <<host::binary-size(host_size), port::size(32)>>} =  :gen_tcp.recv(socket, host_size + 4, timeout)
      {node_id, host, port}
    end)
    {:ok, <<num_topic_metadatas::size(32)>>} = :gen_tcp.recv(socket, 4, timeout)
    topic_metadatas = Enum.map(1..num_topic_metadatas, fn(_) ->
      {:ok, <<topic_error_code::size(16), topic_size::size(16)>>} = :gen_tcp.recv(socket, 4, timeout)
      {:ok, <<topic::binary-size(topic_size), num_partition_metadatas::size(32)>>} = :gen_tcp.recv(socket, topic_size + 4, timeout)
      partition_metadatas = Enum.map(1..num_partition_metadatas, fn(_) ->
        {:ok, <<partition_error_code::size(16), partition_id::size(32),
                leader::size(32), num_replicas::size(32)>>} = :gen_tcp.recv(socket, 2 + 4 + 4 + 4, timeout)
        {:ok, encoded_replicas} = :gen_tcp.recv(socket, num_replicas * 4, timeout)
        replicas = decode_array32(encoded_replicas, [])
        {:ok, <<num_isr::size(32)>>} = :gen_tcp.recv(socket, 4, timeout)
        {:ok, encoded_isr} = :gen_tcp.recv(socket, num_isr * 4, timeout)
        isr = decode_array32(encoded_isr, [])
        {partition_error_code, partition_id, leader, replicas, isr}
      end)
      {topic_error_code, topic, partition_metadatas}
    end)

    {correlation_id, brokers, topic_metadatas}
  end

  def fetch(socket, fetch_requests, max_wait_time, min_bytes, timeout, correlation_id \\ 0, client_id \\ "kafkaex") do
    request = encode_fetch_requests(correlation_id, client_id, fetch_requests, max_wait_time, min_bytes)
    :ok = :gen_tcp.send(socket, request)
    recv_fetch_response(socket, timeout)
  end

  def encode_fetch_requests(correlation_id, client_id, fetch_requests, max_wait_time, min_bytes) do
    api_version = 0
    api_key = 1
    replica_id = -1
    client_id_size = byte_size(client_id)
    {topic_partitions_size, encoded_topic_partitions} = encode_fetch_topic_partitions(fetch_requests, 0, 0, [])
    request_size = 2 + 2 + 4 + 2 + client_id_size + 4 + 4 + 4 + topic_partitions_size

    [<<request_size::size(32), api_key::size(16), api_version::size(16),
       correlation_id::size(32), client_id_size::size(16), client_id::binary-size(client_id_size), replica_id::size(32),
       max_wait_time::size(32), min_bytes::size(32)>>] ++ encoded_topic_partitions
  end

  def encode_fetch_topic_partitions([{topic, partitions} | topic_partitions], size, count, encoded) do
    {partitions_size, encoded_partitions} = encode_fetch_partitions(partitions, 0, 0, [])
    topic_size = byte_size(topic)
    encode_fetch_topic_partitions(topic_partitions, size + 2 + topic_size + partitions_size, count + 1, [encoded, <<topic_size::size(16), topic::binary>>, encoded_partitions])
  end

  def encode_fetch_topic_partitions([], size, count, encoded) do
    {size + 4, List.flatten [<<count::size(32)>>, encoded]}
  end

  def encode_fetch_partitions([{partition, fetch_offset, max_bytes} | partitions], size, count, encoded) do
    encode_fetch_partitions(partitions, size + 4 + 8 + 4, count + 1, [encoded, <<partition::size(32), fetch_offset::size(64), max_bytes::size(32)>>])
  end

  def encode_fetch_partitions([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  def recv_fetch_response(socket, timeout) do
    {:ok, <<response_size::size(32)>>} = :gen_tcp.recv(socket, 4, timeout)
    {:ok, <<correlation_id::size(32), num_topics::size(32)>>} = :gen_tcp.recv(socket, 8, timeout)
    {correlation_id, Enum.map(1..num_topics, fn(_) ->
      {:ok, <<topic_size::size(16)>>} = :gen_tcp.recv(socket, 2, timeout)
      {:ok, <<topic::binary-size(topic_size), num_partitions::size(32)>>} = :gen_tcp.recv(socket, topic_size + 4, timeout)
      {topic, Enum.map(1..num_partitions, fn(_) ->
        {:ok, <<partition::size(32), error_code::size(16), highwater_mark_offset::size(64), message_set_size::(32)>>} = :gen_tcp.recv(socket, 4 + 2 + 8 + 4, timeout)
        decoded_message_set = case message_set_size do
                                0 -> []
                                message_set_size ->
                                  {:ok, encoded_message_set} = :gen_tcp.recv(socket, message_set_size, timeout) # Might need to batch this read if there are lots of messages or big ones
                                  decode_message_set(encoded_message_set, [])
                              end
        {partition, error_code, highwater_mark_offset, decoded_message_set}
      end)}
    end)}
  end

  def decode_message_set(<<offset::size(64), message_size::size(32), crc::size(32), magic::size(8), attributes::size(8), key_size::size(32)-big-signed-integer, rest::binary>>, messages) do
    case key_size do
      -1 ->
        <<value_size::size(32), rest2::binary>> = rest
        <<value::binary-size(value_size), rest3::binary>> = rest2
        # Check crc, magic, compression etc
        decode_message_set(rest3, [messages, {offset, value}])
      key_size ->
        <<key::binary-size(key_size), value_size::size(32), rest2::binary>> = rest
        <<value::binary-size(value_size), rest3::binary>> = rest2
        # Check crc, magic, compression etc
        decode_message_set(rest3, [messages, {offset, key, value}])
      end
  end

  def decode_message_set(<<>>, messages) do
    List.flatten(messages)
  end

  def produce(socket, produce_requests, num_acks, ack_timeout, correlation_id \\ 0, client_id \\ "kafkex") do
    request = encode_produce_requests(correlation_id, client_id, num_acks, ack_timeout, produce_requests)
    :ok = :gen_tcp.send(socket, request)
    recv_produce_response(socket, ack_timeout) # different param for socket timeout and ack timeout?
  end

  def encode_produce_requests(correlation_id, client_id, num_acks, ack_timeout, produce_requests) do
    api_key = 0
    api_version = 0
    {encoded_produce_size, encoded_produce} = encode_produce_messages(produce_requests, 0, 0, [])
    client_id_size = byte_size(client_id)
    request_size = 2 + 2 + 4 + 2 + client_id_size + 2 + 4 + encoded_produce_size
    [<<request_size::size(32), api_key::size(16), api_version::size(16),
      correlation_id::size(32), client_id_size::size(16)>>,
      client_id, <<num_acks::size(16), ack_timeout::size(32)>>, List.flatten(encoded_produce)]
  end

  def encode_produce_messages([{topic, partition_msgs} | produce_requests], size, count, encoded) do
    topic_size = byte_size(topic)
    {partitions_size, encoded_partitions} = encode_produce_partitions(partition_msgs, 0, 0, [])
    encode_produce_messages(produce_requests, size + 2 + topic_size + partitions_size, count + 1, [encoded,
      <<topic_size::size(16), topic::binary-size(topic_size)>>, encoded_partitions])
  end

  def encode_produce_messages([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  def encode_produce_partitions([{partition, msgs} | partition_msgs], size, count, encoded) do
    {msg_set_size, encoded_msg_set} = encode_produce_msg_set(msgs, 0, [])
    encode_produce_partitions(partition_msgs, size + 4 + 4 + msg_set_size, count + 1, [encoded, <<partition::size(32), msg_set_size::size(32)>>, encoded_msg_set])
  end

  def encode_produce_partitions([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  def encode_produce_msg_set([elem | msgs], size, encoded) do
    {key, msg} = case elem do
      {key, msg} -> {key, msg}
      msg -> {"", msg}
    end
    key_size = byte_size(key)
    msg_size = byte_size(msg)
    full_msg_size = 4 + 1 + 1 + 4 + key_size + 4 + msg_size
    magic = 0
    attributes = 0
    data = <<magic, attributes, key_size::size(32), key::binary-size(key_size), msg_size::size(32), msg::binary-size(msg_size)>>
    crc = :erlang.crc32(data)
    encode_produce_msg_set(msgs, size + 8 + 4 + full_msg_size, [encoded, <<0::size(64), full_msg_size::size(32), crc::size(32)>>, data])
  end

  def encode_produce_msg_set([], size, encoded) do
    {size, encoded}
  end

  def recv_produce_response(socket, timeout) do
    {:ok, <<response_size::size(32)>>} = :gen_tcp.recv(socket, 4, timeout)
    {:ok, <<correlation_id::size(32), num_topics::size(32)>>} = :gen_tcp.recv(socket, 8, timeout)
    Enum.map(1..num_topics, fn(_) ->
      {:ok, <<topic_size::size(16)>>} = :gen_tcp.recv(socket, 2, timeout)
      {:ok, <<topic::binary-size(topic_size), num_partitions::size(32)>>} = :gen_tcp.recv(socket, topic_size + 4, timeout)
      {topic, Enum.map(1..num_partitions, fn(_) ->
        {:ok, <<partition::size(32), error_code::size(16), offset::size(64)>>} = :gen_tcp.recv(socket, 4 + 2 + 8, timeout)
        {partition, error_code, offset}
      end)}
    end)
  end
end

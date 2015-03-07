defmodule Kafkex.Produce do
  @api_key 0
  @api_version 0

  def encode(correlation_id, client_id, num_acks, ack_timeout, produce_requests) do
    {encoded_produce_size, encoded_produce} = encode_produce_messages(produce_requests, 0, 0, [])
    client_id_size = byte_size(client_id)
    request_size = 2 + 2 + 4 + 2 + client_id_size + 2 + 4 + encoded_produce_size
    [<<request_size::size(32), @api_key::size(16), @api_version::size(16),
      correlation_id::size(32), client_id_size::size(16)>>,
      client_id, <<num_acks::size(16), ack_timeout::size(32)>>, List.flatten(encoded_produce)]
  end

  def recv(socket, timeout) do
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

  defp encode_produce_messages([{topic, partition_msgs} | produce_requests], size, count, encoded) do
    topic_size = byte_size(topic)
    {partitions_size, encoded_partitions} = encode_produce_partitions(partition_msgs, 0, 0, [])
    encode_produce_messages(produce_requests, size + 2 + topic_size + partitions_size, count + 1, [encoded,
      <<topic_size::size(16), topic::binary-size(topic_size)>>, encoded_partitions])
  end

  defp encode_produce_messages([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  defp encode_produce_partitions([{partition, msgs} | partition_msgs], size, count, encoded) do
    {msg_set_size, encoded_msg_set} = encode_produce_msg_set(msgs, 0, [])
    encode_produce_partitions(partition_msgs, size + 4 + 4 + msg_set_size, count + 1, [encoded, <<partition::size(32), msg_set_size::size(32)>>, encoded_msg_set])
  end

  defp encode_produce_partitions([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  defp encode_produce_msg_set([elem | msgs], size, encoded) do
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

  defp encode_produce_msg_set([], size, encoded) do
    {size, encoded}
  end
end

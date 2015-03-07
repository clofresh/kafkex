defmodule Kafkex.Fetch do
  @api_key 1
  @api_version 0

  def encode(correlation_id, client_id, fetch_requests, max_wait_time, min_bytes) do
    replica_id = -1
    client_id_size = byte_size(client_id)
    {topic_partitions_size, encoded_topic_partitions} = encode_fetch_topic_partitions(fetch_requests, 0, 0, [])
    request_size = 2 + 2 + 4 + 2 + client_id_size + 4 + 4 + 4 + topic_partitions_size

    [<<request_size::size(32), @api_key::size(16), @api_version::size(16),
       correlation_id::size(32), client_id_size::size(16), client_id::binary-size(client_id_size), replica_id::size(32),
       max_wait_time::size(32), min_bytes::size(32)>>] ++ encoded_topic_partitions
  end

  def recv(socket, timeout) do
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

  defp encode_fetch_topic_partitions([{topic, partitions} | topic_partitions], size, count, encoded) do
    {partitions_size, encoded_partitions} = encode_fetch_partitions(partitions, 0, 0, [])
    topic_size = byte_size(topic)
    encode_fetch_topic_partitions(topic_partitions, size + 2 + topic_size + partitions_size, count + 1, [encoded, <<topic_size::size(16), topic::binary>>, encoded_partitions])
  end

  defp encode_fetch_topic_partitions([], size, count, encoded) do
    {size + 4, List.flatten [<<count::size(32)>>, encoded]}
  end

  defp encode_fetch_partitions([{partition, fetch_offset, max_bytes} | partitions], size, count, encoded) do
    encode_fetch_partitions(partitions, size + 4 + 8 + 4, count + 1, [encoded, <<partition::size(32), fetch_offset::size(64), max_bytes::size(32)>>])
  end

  defp encode_fetch_partitions([], size, count, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  def decode_message_set(<<offset::size(64), message_size::size(32), crc::size(32), magic::size(8), attributes::size(8), key_size::size(32)-big-signed-integer, key::binary-size(key_size), value_size::size(32), value::binary-size(value_size), rest::binary>>, messages) when key_size <= 0 do
    # Check crc, magic, compression etc
    decode_message_set(rest, [messages, {offset, value}])
  end

  def decode_message_set(<<offset::size(64), message_size::size(32), crc::size(32), magic::size(8), attributes::size(8), key_size::size(32)-big-signed-integer, key::binary-size(key_size), value_size::size(32), value::binary-size(value_size), rest::binary>>, messages) when key_size > 0 do
    # Check crc, magic, compression etc
    decode_message_set(rest, [messages, {offset, key, value}])
  end

  # This will match when rest is empty or if it's a partial message
  def decode_message_set(rest, messages) do
    List.flatten(messages)
  end
end

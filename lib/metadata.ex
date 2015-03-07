defmodule Kafkex.Metadata do
  @api_key 3
  @api_version 0

  def encode(correlation_id, client_id, topics) do
    {encoded_topics_size, encoded_topics} = encode_metadata_topics(topics)
    client_id_size = byte_size(client_id)
    request_size = 2 + 2 + 4 + 2 + client_id_size + encoded_topics_size
    [<<request_size::size(32), @api_key::size(16), @api_version::size(16),
      correlation_id::size(32), client_id_size::size(16)>>,
      client_id, encoded_topics]
  end

  def recv(socket, timeout) do
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

  defp encode_metadata_topics(topics) do
    encode_metadata_topics(topics, 0, 0, [])
  end

  defp encode_metadata_topics([topic | topics], count, size, encoded) when is_binary(topic) do
    topic_size = byte_size(topic)
    encode_metadata_topics(topics, count + 1, size + 2 + topic_size, [encoded, <<topic_size::size(16)>>, topic])
  end

  defp encode_metadata_topics([], count, size, encoded) do
    {size + 4, [<<count::size(32)>>, encoded]}
  end

  defp decode_array32(<<>>, vals) do
    List.flatten(vals)
  end

  defp decode_array32(<<val::size(32), rest::binary>>, vals) do
    decode_array32(rest, [vals, val])
  end

end

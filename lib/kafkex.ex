defmodule Kafkex do
  alias Kafkex.Metadata
  alias Kafkex.Fetch
  alias Kafkex.Produce

  @client_id "kafkex"

  @spec connect({String.t, number}, number) :: :gen_tcp.socket()
  def connect({host, port}, timeout \\ 500) when is_integer(port) and is_integer(port) do
    options = [:binary, {:active, :false}, {:packet, :raw}]
    {:ok, socket} = :gen_tcp.connect(host, port, options, timeout)
    socket
  end

  @spec metadata(:gen_tcp.socket(), [String.t], number, number, String.t) :: Metadata.t
  def metadata(socket, topics, timeout, correlation_id \\ 0, client_id \\ @client_id) do
    request = Metadata.encode(correlation_id, client_id, topics)
    :ok = :gen_tcp.send(socket, request)
    Metadata.recv(socket, timeout)
  end

  def fetch(socket, fetch_requests, max_wait_time, min_bytes, timeout, correlation_id \\ 0, client_id \\ @client_id) do
    request = Fetch.encode(correlation_id, client_id, fetch_requests, max_wait_time, min_bytes)
    :ok = :gen_tcp.send(socket, request)
    Fetch.recv(socket, timeout)
  end

  def produce(socket, produce_requests, num_acks, ack_timeout, correlation_id \\ 0, client_id \\ @client_id) do
    request = Produce.encode(correlation_id, client_id, num_acks, ack_timeout, produce_requests)
    :ok = :gen_tcp.send(socket, request)
    Produce.recv(socket, ack_timeout) # different param for socket timeout and ack timeout?
  end
end

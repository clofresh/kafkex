defmodule KafkexTest do
  use ExUnit.Case
  doctest Kafkex

  test "metadata request" do
    socket = Kafkex.connect({'localhost', 19092})
    metadata = Kafkex.metadata(socket, ["test0", "test1"], 1000, 10)
    assert metadata == {10, [{0, "carlo-arch.localdomain", 19092}],
                         [{0, "test0", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]},
                          {0, "test1", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]}]}
    :gen_tcp.close(socket)
  end

  test "fetch request" do
    socket = Kafkex.connect({'localhost', 19092})
    messages = Kafkex.fetch(socket, [{"test0", [{0, 0, 1024*1024}, {1, 0, 1024*1024}]},
                                     {"test1", [{0, 0, 1024*1024}, {1, 0, 1024*1024}]}],
                            1000, 1024, 5000, 12)
    assert messages == {12,
            [{"test1",
              [{0, 0, 0, []},
               {1, 0, 4, [{0, "qwe"}, {1, "rqw"}, {2, "543"}, {3, "4523"}]}]},
             {"test0",
              [{0, 0, 0, []},
               {1, 0, 4, [{0, "test"}, {1, "test"}, {2, "adf"}, {3, "asdf"}]}]}]}
    :gen_tcp.close(socket)
  end
end

defmodule KafkexTest do
  use ExUnit.Case
  doctest Kafkex

  test "metadata request" do
    socket = Kafkex.connect({'localhost', 19092})
    try do
      metadata = Kafkex.metadata(socket, ["test0", "test1"], 1000, 10)
      assert metadata == {10, [{0, "carlo-arch.localdomain", 19092}],
                           [{0, "test0", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]},
                            {0, "test1", [{0, 1, 0, [0], [0]}, {0, 0, 0, [0], [0]}]}]}
    after
      :gen_tcp.close(socket)
    end
  end

  test "fetch request" do
    socket = Kafkex.connect({'localhost', 19092})
    try do
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
    after
      :gen_tcp.close(socket)
    end
  end

  test "produce request" do
    socket = Kafkex.connect({'localhost', 19092})
    try do
      response = Kafkex.produce(socket, [{"test0", [{0, ["yo", "sup"]},
                                                    {1, ["not much"]}]},
                                          {"test1", [{1, ["blah"]}]}],
                                1, 1000)
      [{"test1", [{1, 0, o1}]}, {"test0", [{1, 0, o01}, {0, 0, o00}]}] = response
      assert o1 > 0
      assert o01 > 0
      assert o00 > 0
    after
      :gen_tcp.close(socket)
    end
  end
end

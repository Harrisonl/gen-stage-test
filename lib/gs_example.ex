alias Experimental.GenStage

defmodule Go do
  def go do
    :ets.new(:logs, [:named_table, :public])

    {:ok, producer} = GenStage.from_enumerable(File.stream!("nasa.log"), name: __MODULE__)
    {:ok, consumer1} = GsConsumer.start_link("1")
    {:ok, consumer1} = GsConsumer.start_link("1")
    {:ok, consumer1} = GsConsumer.start_link("1")
    {:ok, consumer1} = GsConsumer.start_link("1")
  end

end

defmodule GsConsumer do
  use GenStage

  def start_link(num) do
    GenStage.start_link(__MODULE__, [num])
  end

  def init([num]) do
    {:consumer, num, subscribe_to: [Go]}
  end

  def handle_events(lines, _from, num) do
      lines
      |> Enum.each(fn(line) ->
        reg = ~r/GET \/[a-z]{0,10}\b([-a-zA-Z0-9@:%_\+.~#?&\/]*)/
        case Regex.run(reg, line) do
          nil -> ""
          [h | tail] -> 
            x = String.to_atom(h) 
            :ets.update_counter(:logs, x, 1, {x, 1})
        end
      end)
    {:noreply, [], num}
  end
end


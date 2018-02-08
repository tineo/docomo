defmodule Docomo.Worker do
  use GenServer

  ## Client API

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: :publisher)
  end

  def publish(message) do
    IO.puts "handling cast.. "
    GenServer.cast(:publisher, {:publish, message})
  end

  ## Server Callbacks

  def init(:ok) do
    {:ok, connection} = AMQP.Connection.open
    {:ok, channel} = AMQP.Channel.open(connection)
    AMQP.Queue.declare(channel, "tracks")
    {:ok, %{channel: channel, connection: connection} }
  end

  def handle_cast({:publish, message}, state) do
    AMQP.Basic.publish(state.channel, "", "tracks", message)
    {:noreply, state}
  end

  def terminate(_reason, state) do
    AMQP.Connection.close(state.connection)
  end
end
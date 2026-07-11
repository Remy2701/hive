import gleam/option

pub type Strategy {
  LastInFirstOut
  FirstInFirstOut
}

pub type Builder {
  Builder(size: Int, strategy: Strategy, timeout: option.Option(Int))
}

pub fn new() -> Builder {
  Builder(size: 10, strategy: LastInFirstOut, timeout: option.None)
}

pub fn with_size(builder: Builder, size: Int) -> Builder {
  Builder(..builder, size: size)
}

pub fn with_strategy(builder: Builder, strategy: Strategy) -> Builder {
  Builder(..builder, strategy: strategy)
}

pub fn with_timeout(builder: Builder, timeout: Int) -> Builder {
  Builder(..builder, timeout: option.Some(timeout))
}

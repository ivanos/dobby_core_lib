-module(dby_test_mock).

-compile(export_all).

search_fn(_, _, _, _) -> exit(needs_mock).

delta_fn(_, _) -> exit(needs_mock).

delivery_fn(_) -> exit(needs_mock).

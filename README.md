A project for splitting large columnar ASCII formatted files into smaller ones by key.
Utilizes memory maps to avoid loading the set into memory like polars does in the unsorted case.

This removes the type safety and interpretation performed by dataframe libraries like Polars, and is only suitable when
the boundary conditions are well established.
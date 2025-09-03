// generate_parquet.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func createSimpleFile() {
	fmt.Println("Creating simple.parquet...")
	
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	// Add data
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues([]int32{25, 30, 35, 28, 32}, nil)
	
	record := builder.NewRecord()
	defer record.Release()
	
	writeParquetFile("simple.parquet", schema, record)
}

func createTypesFile() {
	fmt.Println("Creating types.parquet...")
	
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32},
		{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64},
		{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64},
		{Name: "string_col", Type: arrow.BinaryTypes.String},
		{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	// Add data with different types
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 200, 300}, nil)
	builder.Field(2).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2, 3.3}, nil)
	builder.Field(3).(*array.Float64Builder).AppendValues([]float64{10.1, 20.2, 30.3}, nil)
	builder.Field(4).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	builder.Field(5).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	
	record := builder.NewRecord()
	defer record.Release()
	
	writeParquetFile("types.parquet", schema, record)
}

func createMinimalFile() {
	fmt.Println("Creating minimal.parquet...")
	
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	// Single column, single row
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{42}, nil)
	
	record := builder.NewRecord()
	defer record.Release()
	
	writeParquetFile("minimal.parquet", schema, record)
}

func writeParquetFile(filename string, schema *arrow.Schema, record arrow.Record) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", filename, err)
	}
	defer file.Close()
	
	// Use default properties - this should work with any version
	writer, err := pqarrow.NewFileWriter(schema, file, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Fatalf("Failed to create writer for %s: %v", filename, err)
	}
	defer writer.Close()
	
	err = writer.Write(record)
	if err != nil {
		log.Fatalf("Failed to write record to %s: %v", filename, err)
	}
	
	fmt.Printf("Successfully created %s\n", filename)
}

func main() {
	fmt.Println("Generating spec-compliant Parquet test files...")
	
	createMinimalFile()   // Simplest possible case
	createSimpleFile()    // Basic multi-column case  
	createTypesFile()     // All major data types
	
	fmt.Println("Done! Generated:")
	fmt.Println("  - minimal.parquet (1 column, 1 row)")
	fmt.Println("  - simple.parquet (3 columns, 5 rows)")
	fmt.Println("  - types.parquet (6 columns, 3 rows, all major types)")
}

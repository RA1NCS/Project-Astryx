import time
import json
import statistics

# Docling imports
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption


def benchmark_docling_processing(config_name, opts, num_runs=5):
    """Benchmark Docling processing with given options"""
    times = []
    
    for run in range(num_runs):
        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
        )
        
        start_time = time.time()
        res = converter.convert(
            "image_document.pdf",
            page_range=(1, 4),
        )
        end_time = time.time()
        
        processing_time = end_time - start_time
        times.append(processing_time)
        print(f"{config_name} - Run {run + 1}: {processing_time:.2f} seconds")
    
    avg_time = statistics.mean(times)
    print(f"{config_name} - Average time: {avg_time:.2f} seconds")
    print(f"{config_name} - Min: {min(times):.2f}s, Max: {max(times):.2f}s")
    print("-" * 50)
    
    return avg_time, res


# Configuration 1: OCR only
opts_ocr = PdfPipelineOptions(
    do_ocr=True,
    do_table_structure=False,
    do_picture_description=False,
    generate_parsed_pages=False,
    generate_page_images=False,
    generate_picture_images=False,
)

# Configuration 2: Table structure only
opts_table = PdfPipelineOptions(
    do_ocr=False,
    do_table_structure=True,
    do_picture_description=False,
    generate_parsed_pages=False,
    generate_page_images=False,
    generate_picture_images=False,
)

# Configuration 3: Generate parsed images only
opts_images = PdfPipelineOptions(
    do_ocr=False,
    do_table_structure=False,
    do_picture_description=False,
    generate_parsed_pages=True,
    generate_page_images=False,
    generate_picture_images=False,
)

# Configuration 4: All false
opts_minimal = PdfPipelineOptions(
    do_ocr=False,
    do_table_structure=False,
    do_picture_description=False,
    generate_parsed_pages=False,
    generate_page_images=False,
    generate_picture_images=False,
)

# Run benchmarks
print("Starting Docling Processing Benchmarks")
print("=" * 50)

avg_ocr, res_ocr = benchmark_docling_processing("OCR Only", opts_ocr)
avg_table, res_table = benchmark_docling_processing("Table Structure Only", opts_table)
avg_images, res_images = benchmark_docling_processing("Generate Parsed Images Only", opts_images)
avg_minimal, res_minimal = benchmark_docling_processing("All False (Minimal)", opts_minimal)

# Summary
print("\nBenchmark Summary:")
print("=" * 50)
print(f"OCR Only:                    {avg_ocr:.2f}s")
print(f"Table Structure Only:        {avg_table:.2f}s")
print(f"Generate Parsed Images Only: {avg_images:.2f}s")
print(f"All False (Minimal):         {avg_minimal:.2f}s")

# Save output from minimal config to JSON
output_data = res_minimal.document.export_to_dict()
with open("try_output.json", "w") as f:
    json.dump(output_data, f, indent=2)

print("\nOutput from minimal config saved to try_output.json")

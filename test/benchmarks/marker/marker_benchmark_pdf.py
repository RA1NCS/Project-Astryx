import os
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict


def convert_pdf_to_json(input_pdf_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    # Only minimal config: JSON output (images extracted by default)
    config_dict = {"output_format": "json"}
    parser = ConfigParser(config_dict)

    converter = PdfConverter(
        artifact_dict=create_model_dict(),
        config=parser.generate_config_dict(),
        renderer=parser.get_renderer(),
        processor_list=parser.get_processors(),
    )

    rendered = converter(input_pdf_path)
    # Use Pydantic’s JSON dump to avoid unhashable-dict errors
    json_str = rendered.model_dump_json(indent=2)

    json_path = os.path.join(output_dir, "output.json")
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(json_str)

    print(f"✔ Saved JSON: {json_path}")
    print(f"✔ Images (if any) → {os.path.join(output_dir, 'images')}")


if __name__ == "__main__":
    # Example invocation
    pdf_file = "table_document.pdf"
    out_directory = "out_dir"
    convert_pdf_to_json(pdf_file, out_directory)

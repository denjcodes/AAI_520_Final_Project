#!/usr/bin/env python3
"""
Create combined PDF with APA 7 formatting for AAI_520 Final Project
Combines v2 notebook PDF with adapter source code PDFs
Follows APA 7th edition formatting guidelines
"""

import os
import subprocess
import logging
from pathlib import Path
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Preformatted
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from PyPDF2 import PdfMerger

# Quiet noisy logs from weasyprint
for module in ["weasyprint", "fontTools", "fontTools.subset", "fontTools.ttLib"]:
    logging.getLogger(module).setLevel(logging.CRITICAL)

# File paths
adapters_dir = Path('./adapters')
output_dir = Path('.')
v2_notebook_file = output_dir / 'Agentic_Invest_Research_System_Combined_v2.ipynb'
v2_notebook_pdf = output_dir / 'Module_7_Agentic_Invest_Research_System_Combined_v2.pdf'

# List of adapter files in order
adapter_files = [
    '__init__.py',
    'base.py',
    'yahoo.py',
    'news.py',
    'sec.py'
]

def add_apa_header(canvas, doc, running_head=""):
    """Add APA 7 style header with running head and page number"""
    canvas.saveState()
    # Running head (left-aligned, top 0.5 inch from edge)
    canvas.setFont('Times-Roman', 12)
    canvas.drawString(1*inch, letter[1] - 0.5*inch, running_head.upper())
    # Page number (right-aligned)
    page_num = canvas.getPageNumber()
    canvas.drawRightString(letter[0] - 1*inch, letter[1] - 0.5*inch, str(page_num))
    canvas.restoreState()

def convert_notebook_to_pdf_apa(notebook_path: Path, output_pdf: Path, temp_dir: Path) -> bool:
    """Convert Jupyter notebook to APA 7 formatted PDF using nbconvert and weasyprint"""
    try:
        from weasyprint import HTML
    except ImportError:
        print("✗ Error: weasyprint not installed. Install with: pip install weasyprint")
        return False

    html_path = temp_dir / f"{notebook_path.stem}.html"
    fixed_html_path = temp_dir / f"{notebook_path.stem}_apa.html"

    # APA 7 CSS styling
    apa_css = """
    <style>
        /* APA 7 Page Setup */
        @page {
            size: 8.5in 11in;
            margin: 1in 1in 1in 1in;
        }

        /* Running header using position fixed */
        @media print {
            .apa-header {
                position: running(header);
            }
        }

        /* Base Typography - APA 7 requires Times New Roman, 12pt, double-spaced */
        body {
            font-family: "Times New Roman", Times, serif;
            font-size: 10pt;  /* Reduced from 12pt for better fit */
            line-height: 1.5;  /* Reduced from 2.0 to fit more content */
            color: #000000;
            margin: 0;
            padding: 0;
            overflow-wrap: break-word;
        }

        /* Ensure content breaks across pages */
        * {
            box-sizing: border-box;
        }

        /* APA Heading Levels */
        h1 {
            font-family: "Times New Roman", Times, serif;
            font-size: 11pt;
            font-weight: bold;
            text-align: center;
            line-height: 1.5;
            margin-top: 0;
            margin-bottom: 8pt;
            page-break-after: avoid;
        }

        h2 {
            font-family: "Times New Roman", Times, serif;
            font-size: 10pt;
            font-weight: bold;
            text-align: left;
            line-height: 1.5;
            margin-top: 8pt;
            margin-bottom: 6pt;
            page-break-after: avoid;
        }

        h3 {
            font-family: "Times New Roman", Times, serif;
            font-size: 10pt;
            font-weight: bold;
            text-align: left;
            line-height: 1.5;
            margin-top: 6pt;
            margin-bottom: 4pt;
            page-break-after: avoid;
        }

        /* Paragraphs */
        p {
            font-family: "Times New Roman", Times, serif;
            font-size: 10pt;
            line-height: 1.5;
            text-align: left;
            margin: 4pt 0;
            orphans: 2;
            widows: 2;
        }

        /* Code blocks - smaller font for readability */
        pre, code, .jp-InputArea, .input_area {
            font-family: "Courier New", Courier, monospace;
            font-size: 7pt;
            line-height: 1.1;
            background-color: #f8f8f8;
            padding: 4pt;
            margin: 4pt 0;
            border: 0.5pt solid #dddddd;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-wrap: break-word;
            page-break-inside: avoid;
            max-height: none;
        }

        /* Output areas */
        .jp-OutputArea, .output_area {
            font-family: "Courier New", Courier, monospace;
            font-size: 7pt;
            line-height: 1.1;
            margin: 4pt 0;
            page-break-inside: avoid;
            max-height: 400pt;  /* Limit output height */
            overflow: hidden;
        }

        /* Truncate very long outputs */
        .jp-OutputArea-output, .output_subarea {
            max-height: 400pt;
            overflow: hidden;
        }

        /* Hide prompts (In[1], Out[1], etc.) */
        .prompt, .jp-InputPrompt, .jp-OutputPrompt, .input_prompt, .output_prompt {
            display: none !important;
        }

        /* Tables - APA formatting */
        table {
            font-family: "Times New Roman", Times, serif;
            font-size: 8pt;
            line-height: 1.2;
            border-collapse: collapse;
            margin: 6pt 0;
            width: 100%;
            page-break-inside: auto;
        }

        tr {
            page-break-inside: avoid;
            page-break-after: auto;
        }

        th, td {
            border: 0.5pt solid #666666;
            padding: 2pt;
            text-align: left;
            word-wrap: break-word;
        }

        th {
            font-weight: bold;
            border-bottom: 1pt solid #000000;
            background-color: #f0f0f0;
        }

        /* Lists */
        ul, ol {
            font-family: "Times New Roman", Times, serif;
            font-size: 10pt;
            line-height: 1.5;
            margin-left: 0.3in;
            margin-top: 4pt;
            margin-bottom: 4pt;
        }

        li {
            margin-bottom: 2pt;
        }

        /* Images - limit size to fit on page */
        img {
            max-width: 100%;
            max-height: 400pt;
            height: auto;
            display: block;
            margin: 6pt auto;
            page-break-inside: avoid;
        }

        /* Remove cell borders and backgrounds */
        .cell, .jp-Cell {
            border: none !important;
            background: transparent !important;
            page-break-inside: auto;
        }

        /* Container spacing */
        #notebook-container {
            padding: 0 !important;
            margin: 0 !important;
        }

        .container {
            max-width: 100% !important;
            padding: 0 !important;
        }

        body, html, main, #notebook, #notebook-container, .container {
            overflow: visible !important;
            overflow-y: visible !important;
            overflow-x: visible !important;
            height: auto !important;
            max-height: none !important;
            min-height: auto !important;
        }

        /* Allow content to flow across pages */
        div, section, article {
            overflow: visible !important;
        }
    </style>
    """

    print(f"  1. Converting notebook to HTML...")
    try:
        # Run nbconvert to generate HTML with basic template
        result = subprocess.run(
            [
                "jupyter", "nbconvert",
                "--to", "html",
                "--template", "basic",  # Use basic template for cleaner HTML
                "--output", str(html_path.name),
                "--output-dir", str(temp_dir),
                str(notebook_path)
            ],
            check=True,
            capture_output=True,
            text=True
        )

        if result.stderr and "error" in result.stderr.lower():
            print(f"     Warning: {result.stderr[:200]}")

    except subprocess.CalledProcessError as e:
        # If basic template fails, try classic
        print(f"     Basic template failed, trying classic...")
        try:
            result = subprocess.run(
                [
                    "jupyter", "nbconvert",
                    "--to", "html",
                    "--template", "classic",
                    "--output", str(html_path.name),
                    "--output-dir", str(temp_dir),
                    str(notebook_path)
                ],
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e2:
            print(f"✗ nbconvert failed: {e2.stderr}")
            return False
    except FileNotFoundError:
        print("✗ Error: jupyter nbconvert not found. Install with: pip install nbconvert")
        return False

    print(f"  2. Injecting APA 7 CSS styling...")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        # Inject APA CSS before </head>
        html_content = html_content.replace("</head>", f"{apa_css}</head>", 1)

        with open(fixed_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

    except Exception as e:
        print(f"✗ Error injecting CSS: {e}")
        return False

    print(f"  3. Converting to PDF with APA 7 formatting...")
    try:
        HTML(str(fixed_html_path)).write_pdf(str(output_pdf))
        print(f"✓ Created APA 7 formatted notebook PDF: {output_pdf}")
        return True
    except Exception as e:
        print(f"✗ Error generating PDF: {e}")
        return False

def create_title_page(output_file: str):
    """Create APA 7 style title page"""
    doc = SimpleDocTemplate(output_file, pagesize=letter,
                            topMargin=1*inch, bottomMargin=1*inch,
                            leftMargin=1*inch, rightMargin=1*inch)

    styles = getSampleStyleSheet()

    # APA Title style (bold, centered, Title Case)
    title_style = ParagraphStyle(
        'APATitle',
        parent=styles['Normal'],
        fontName='Times-Bold',
        fontSize=12,
        alignment=TA_CENTER,
        spaceAfter=0,
        leading=24  # Double spacing
    )

    # APA Normal style (Times New Roman, 12pt, double-spaced)
    body_style = ParagraphStyle(
        'APABody',
        parent=styles['Normal'],
        fontName='Times-Roman',
        fontSize=12,
        alignment=TA_CENTER,
        leading=24  # Double spacing (2 * 12pt)
    )

    story = []

    # Title page content (APA 7 format)
    # Vertical centering approximation
    story.append(Spacer(1, 2.5*inch))

    story.append(Paragraph("Agentic AI for Financial Analysis:", title_style))
    story.append(Paragraph("A Natural Language Processing Approach to Investment Research", title_style))
    story.append(Spacer(1, 0.5*inch))

    # Author names
    story.append(Paragraph("Pros Loung, Dennis Arapurayil, and Divya Kamath", body_style))
    story.append(Spacer(1, 0.25*inch))

    # Institutional affiliation
    story.append(Paragraph("University of San Diego", body_style))
    story.append(Spacer(1, 0.25*inch))

    # Course information
    story.append(Paragraph("AAI-520: Natural Language Processing and GenAI", body_style))
    story.append(Spacer(1, 0.25*inch))

    # Date
    current_date = datetime.now().strftime("%B %d, %Y")
    story.append(Paragraph(current_date, body_style))

    # Build with header
    doc.build(story, onFirstPage=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"),
              onLaterPages=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"))
    print(f"✓ Created APA title page: {output_file}")

def create_toc_pdf(output_file: str, files_list: list):
    """Create APA 7 style table of contents"""
    doc = SimpleDocTemplate(output_file, pagesize=letter,
                            topMargin=1*inch, bottomMargin=1*inch,
                            leftMargin=1*inch, rightMargin=1*inch)

    styles = getSampleStyleSheet()

    # APA Heading Level 1 (centered, bold, Title Case)
    heading1_style = ParagraphStyle(
        'APAHeading1',
        parent=styles['Normal'],
        fontName='Times-Bold',
        fontSize=12,
        alignment=TA_CENTER,
        spaceAfter=12,
        leading=24
    )

    # TOC entry style (double-spaced)
    toc_style = ParagraphStyle(
        'APATOCEntry',
        parent=styles['Normal'],
        fontName='Times-Roman',
        fontSize=12,
        leftIndent=0.5*inch,
        leading=24,  # Double spacing
        spaceAfter=0
    )

    story = []

    # Table of Contents heading
    story.append(Paragraph("Table of Contents", heading1_style))
    story.append(Spacer(1, 0.25*inch))

    # TOC entries
    for idx, file_info in enumerate(files_list, 1):
        if isinstance(file_info, dict):
            name = file_info['name']
            desc = file_info.get('description', '')
        else:
            name = file_info
            desc = ''

        entry = f"{idx}. {name}"
        if desc:
            entry += f": {desc}"

        story.append(Paragraph(entry, toc_style))

    doc.build(story, onFirstPage=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"),
              onLaterPages=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"))
    print(f"✓ Created APA table of contents: {output_file}")

def create_code_pdf(code_file: Path, output_file: str, title: str):
    """Convert a Python source file to PDF with APA 7 formatting"""
    doc = SimpleDocTemplate(output_file, pagesize=letter,
                            topMargin=1*inch, bottomMargin=1*inch,
                            leftMargin=1*inch, rightMargin=1*inch)

    styles = getSampleStyleSheet()

    # APA Heading Level 1 (centered, bold)
    heading1_style = ParagraphStyle(
        'APAHeading1',
        parent=styles['Normal'],
        fontName='Times-Bold',
        fontSize=12,
        alignment=TA_CENTER,
        spaceAfter=12,
        leading=24
    )

    # Code style (monospace, smaller font for readability)
    code_style = ParagraphStyle(
        'APACode',
        parent=styles['Code'],
        fontSize=9,
        leftIndent=0.5*inch,
        rightIndent=0,
        fontName='Courier',
        leading=11,
        spaceBefore=6,
        spaceAfter=6
    )

    story = []

    # Section heading
    story.append(Paragraph(f"Appendix: {title}", heading1_style))
    story.append(Spacer(1, 0.2*inch))

    # File reference
    file_ref_style = ParagraphStyle(
        'FileRef',
        parent=styles['Normal'],
        fontName='Times-Italic',
        fontSize=12,
        leading=24
    )
    story.append(Paragraph(f"File: {code_file}", file_ref_style))
    story.append(Spacer(1, 0.2*inch))

    # Read and add code
    try:
        with open(code_file, 'r', encoding='utf-8') as f:
            code_content = f.read()

        # Use Preformatted for code rendering
        story.append(Preformatted(code_content, code_style))

        doc.build(story, onFirstPage=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"),
                  onLaterPages=lambda c, d: add_apa_header(c, d, "AGENTIC AI FOR FINANCIAL ANALYSIS"))
        print(f"✓ Created PDF: {output_file}")
        return True
    except Exception as e:
        print(f"✗ Error creating PDF for {code_file}: {e}")
        return False

def combine_pdfs(pdf_list: list, output_file: str):
    """Combine multiple PDFs into one"""
    merger = PdfMerger()

    for pdf_file in pdf_list:
        if os.path.exists(pdf_file):
            print(f"  Adding: {pdf_file}")
            merger.append(pdf_file)
        else:
            print(f"  Warning: {pdf_file} not found, skipping")

    merger.write(output_file)
    merger.close()
    print(f"\n✓ Combined PDF created: {output_file}")

def main():
    print("="*70)
    print("AAI_520 Final Project - APA 7 PDF Generation")
    print("="*70)
    print()

    # Check if notebook PDF exists
    if not v2_notebook_pdf.exists():
        print(f"✗ Error: {v2_notebook_pdf} not found!")
        print("  Please ensure the notebook PDF exists.")
        print("  (You can export from Jupyter: File -> Download as -> PDF)")
        return

    print(f"✓ Found notebook PDF: {v2_notebook_pdf}")
    print()

    # Create output directory for individual PDFs
    temp_pdf_dir = output_dir / 'temp_pdfs'
    temp_pdf_dir.mkdir(exist_ok=True)

    # Create APA title page
    print("Creating APA 7 title page...")
    title_page_pdf = temp_pdf_dir / 'title_page.pdf'
    create_title_page(str(title_page_pdf))
    print()

    # Files list for TOC
    files_list = [
        {'name': 'Main Analysis Notebook',
         'description': 'Agentic Investment Research System Implementation'},
    ]

    # Add adapter files to list
    for adapter_file in adapter_files:
        desc = {
            '__init__.py': 'Adapter module initialization',
            'base.py': 'Base adapter class definitions',
            'yahoo.py': 'Yahoo Finance data adapter',
            'news.py': 'News API data adapter',
            'sec.py': 'SEC filing data adapter'
        }.get(adapter_file, '')

        files_list.append({
            'name': f'Appendix: adapters/{adapter_file}',
            'description': desc
        })

    # Create table of contents PDF
    print("Creating APA 7 table of contents...")
    toc_pdf = temp_pdf_dir / 'toc.pdf'
    create_toc_pdf(str(toc_pdf), files_list)
    print()

    # Convert adapter files to PDF
    print("Converting adapter files to APA 7 formatted PDFs...")
    adapter_pdfs = []
    for adapter_file in adapter_files:
        code_file = adapters_dir / adapter_file
        if code_file.exists():
            pdf_name = f"adapter_{adapter_file.replace('.py', '')}.pdf"
            pdf_path = temp_pdf_dir / pdf_name
            title = f"adapters/{adapter_file}"

            if create_code_pdf(code_file, str(pdf_path), title):
                adapter_pdfs.append(str(pdf_path))
        else:
            print(f"✗ Warning: {code_file} not found")

    print()

    # Combine all PDFs
    print("Combining all PDFs into APA 7 formatted document...")
    all_pdfs = [
        str(title_page_pdf),
        str(toc_pdf),
        str(v2_notebook_pdf),
    ] + adapter_pdfs

    output_pdf = output_dir / 'AAI_520_Final_Project_Complete.pdf'
    combine_pdfs(all_pdfs, str(output_pdf))

    # Summary
    print()
    print("Temporary PDFs saved in:", temp_pdf_dir)
    print()
    print("="*70)
    print(f"✓ COMPLETE: {output_pdf}")
    print("="*70)
    print()
    print("APA 7 Formatting Applied:")
    print("  - APA 7 formatted title page with proper layout")
    print("  - Table of contents with running head")
    print("  - Main notebook PDF (as-is from original export)")
    print("  - Appendices with adapter source code in APA format")
    print("  - All sections properly combined into single document")
    print("="*70)

if __name__ == '__main__':
    main()

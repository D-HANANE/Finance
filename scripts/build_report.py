"""Build a twelve-page report from the reviewed Markdown source."""
from pathlib import Path
from html import escape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak

root = Path(__file__).resolve().parents[1]
output = root/'output/pdf/rapport_plateforme_financiere.pdf'
output.parent.mkdir(parents=True,exist_ok=True)
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='BodyReport',fontName='Helvetica',fontSize=12,
                          leading=18.5,spaceAfter=14,textColor=colors.HexColor('#202020')))
styles['Heading1'].fontName='Helvetica-Bold'
styles['Heading1'].fontSize=21
styles['Heading1'].leading=26
styles['Heading1'].textColor=colors.black
styles['Heading2'].fontName='Helvetica-Bold'
styles['Heading2'].fontSize=17
styles['Heading2'].leading=22
styles['Heading2'].spaceAfter=20
styles['Heading2'].textColor=colors.black


def footer(canvas,document):
    canvas.setFont('Helvetica',9)
    canvas.setFillColor(colors.HexColor('#555555'))
    canvas.drawString(50,32,'Marché financier simulé | Projet académique | Octobre 2026')
    canvas.drawRightString(A4[0]-50,32,str(document.page))


story=[]
pages=(root/'docs/REPORT.md').read_text(encoding='utf-8').split('---page---')
assert len(pages)==12
for index,page in enumerate(pages):
    if index:
        story.append(PageBreak())
    for block in page.strip().split('\n\n'):
        if block.startswith('# '):
            style,text='Heading1',block[2:]
        elif block.startswith('## '):
            style,text='Heading2',block[3:]
        else:
            style,text='BodyReport',block.replace('\n',' ')
        story.append(Paragraph(escape(text),styles[style]))
SimpleDocTemplate(str(output),pagesize=A4,rightMargin=50,leftMargin=50,
                  topMargin=54,bottomMargin=52,title='Plateforme de marché financier simulé',
                  author='Groupe du projet académique').build(story,onFirstPage=footer,onLaterPages=footer)
from pypdf import PdfReader
assert len(PdfReader(output).pages)==12
print(output)

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Union
from datetime import datetime
from groq import Groq
import instructor
import re
import os

client = instructor.from_groq(Groq(api_key=""), mode=instructor.Mode.JSON)

class MetadataResponse(BaseModel):
    scopeAndContent: Optional[str] = Field(None, description="Scope and content of the document.")
    generalNote: Optional[str] = Field(None, description="General notes or remarks about the document.")
    genreAccessPoints: Optional[List[str]] = Field(None, description="Genres or access categories related to the document.")
    culture: Optional[str] = Field(None, description="Cultural context or background related to the content.")
    scientificTerms: Optional[List[str]] = Field(None, description="Scientific or technical terms mentioned in the text.")
    entities: Optional[List[str]] = Field(None, description="Named entities extracted from the document.")
    title: Optional[str] = Field(None, description="Title of the document or record.")
    language: Optional[str] = Field(None, description="Language of the text.")
    topics: Optional[List[str]] = Field(None, description="Main topics or themes of the document.")
    geographic_locations: Optional[List[str]] = Field(None, description="Geographic locations mentioned in the text.")
    dates_mentioned: Optional[List[Union[str, datetime]]] = Field(None, description="Dates mentioned (supports year, month-year, or full date).")
    names_mentioned: Optional[List[str]] = Field(None, description="Names of people or organizations mentioned.")
    places_mentioned: Optional[List[str]] = Field(None, description="Places or landmarks referenced in the document.")



def extract_metadata(text: str):
    messages = [
    {
        "role": "system",
        "content": f"""
        You are a professional archival metadata extraction model.
        
        Your task is to analyze the given text and extract structured archival metadata.
        You MUST return a single valid JSON object that strictly follows the schema below.
        Do NOT include explanations, comments, markdown, or extra text outside JSON.
        
        FIELD DEFINITIONS AND GUIDELINES:
        
        - scopeAndContent:
          A clear, well-written summary of the document.
          It should explain what the document is about, its purpose, and its main themes.
          Write this as a concise paragraph, not bullet points.
          This field should read like an archival abstract.
        
        - generalNote:
          Any additional contextual information that does not fit elsewhere.
          Examples include condition of the document, unusual characteristics, or clarifying remarks.
        
        - genreAccessPoints:
          The type or form of the document.
          Examples: "report", "correspondence", "research paper", "policy document", "technical manual".
        
        - culture:
          Cultural, historical, or social context represented in the document, if identifiable.
          If not explicitly present, infer cautiously or leave empty.
        
        - scientificTerms:
          Domain-specific technical or scientific terms explicitly mentioned in the text.
          Do not invent terms.
        
        - entities:
          persons_name, Important organizations, institutions, movements.
        
        - title:
          The formal or inferred title of the document.
          If no title is present, generate a concise descriptive title based on the content.
        
        - language:
          languages used in the document.
        
        - topics:
          High-level subject themes covered by the document.
          These should be broader than scientificTerms.
        
        - geographic_locations:
          Countries, regions, cities, or specific locations referenced in the text.
        
        - dates_mentioned:
          All explicit dates found in the text.
          Use ISO formats only: YYYY, YYYY-MM, or YYYY-MM-DD.
        
        - names_mentioned:
          Names of people explicitly mentioned in the document.
        
        - places_mentioned:
          Physical places, facilities, landmarks, or sites referenced.
        
        STRICT OUTPUT SCHEMA (JSON ONLY):
        
        {
          "scopeAndContent": "...",
          "generalNote": "...",
          "genreAccessPoints": ["..."],
          "culture": "...",
          "scientificTerms": ["..."],
          "entities": ["..."],
          "title": "...",
          "language": "...",
          "topics": ["..."],
          "geographic_locations": ["..."],
          "dates_mentioned": ["YYYY", "YYYY-MM", "YYYY-MM-DD"],
          "names_mentioned": ["..."],
          "places_mentioned": ["..."]
        }
        
        IMPORTANT RULES:
        - Do not hallucinate facts not present in the text.
        - Use empty strings or empty arrays when information is unavailable.
        - Ensure valid JSON formatting with double quotes only.
        - Ensure scopeAndContent is a meaningful, human-readable summary.
        
        TEXT TO ANALYZE:
        {text}
        """
            }
        ]


    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        response_model=MetadataResponse,
        messages=messages,
        temperature=0.2,
        max_tokens=1000,
    )

    metadata = response
    print("\n--- Final Extracted Metadata (Llama) ---")
    print(f"1. scopeAndContent: {metadata.scopeAndContent}")
    print(f"2. generalNote: {metadata.generalNote}")
    print(f"3. genreAccessPoints: {metadata.genreAccessPoints}")
    print(f"4. culture: {metadata.culture}")
    print(f"5. scientificTerms: {metadata.scientificTerms}")
    print(f"6. entities: {metadata.entities}")
    print(f"7. title: {metadata.title}")
    print(f"8. language: {metadata.language}")
    print(f"9. topics: {metadata.topics}")
    print(f"10. geographic_locations: {metadata.geographic_locations}")
    print(f"11. dates_mentioned: {metadata.dates_mentioned}")
    print(f"12. names_mentioned: {metadata.names_mentioned}")
    print(f"13. places_mentioned: {metadata.places_mentioned}")

    return metadata.model_dump()


if __name__ == "__main__":
    text = """
    The manuscript titled 'Life in Ancient India' discusses cultural evolution between 1947 and 12-10-1569
    It mentions the Mauryan Empire, Ashoka, and scientific developments in metallurgy and astronomy.
    """

    print(type(extract_metadata(text)))

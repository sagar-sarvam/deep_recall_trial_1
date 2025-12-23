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
            You are a metadata extraction model. 
            Extract key fields from the given text and respond in structured JSON strictly following this schema:
            {{
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
            }}

            Text: {text}
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

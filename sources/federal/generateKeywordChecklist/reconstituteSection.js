async function reconstituteSection(pgClient, elementId) {
  const { rows: legalNodesForElement } = await pgClient.query("SELECT element_id, element_type, heading, original_text FROM usc_elements WHERE element_id LIKE $1 ORDER BY element_id", [elementId + "%"]);

  const nodeHierarchy = ["section", "subsection", "paragraph", "subparagraph", "clause", "subclause", "item"];
  let reconstitutedSection = "";

  for (let node of legalNodesForElement) {
    const nodeLevel = nodeHierarchy.indexOf(node.element_type) + 1;

    let nodeMarker = node.element_id.split("/").pop();

    if(node.element_type === "section") {
      nodeMarker = nodeMarker.replace("s", "§ ");
    }
    else {
      nodeMarker = `(${nodeMarker})`;
    }

    let nodeText = "\n\n";

    if(node.heading) {
      nodeText = `${node.heading}\n\n${"\t".repeat(nodeLevel - 1)}${node.original_text}\n\n`;
    }
    else if(node.original_text) {
      nodeText = `${node.original_text}\n\n`;
    }

    reconstitutedSection += `${"\t".repeat(nodeLevel - 1)}${nodeMarker} ${nodeText}`;
  }

  return reconstitutedSection;
}

export { reconstituteSection }
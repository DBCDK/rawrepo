UPDATE records SET mimetype='text/enrichment+marcxchange' WHERE EXISTS (SELECT * FROM relations WHERE agencyid=records.agencyid AND bibliographicrecordid=records.bibliographicrecordid AND refer_bibliographicrecordid=records.bibliographicrecordid);

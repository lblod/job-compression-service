import {
  sparqlEscapeUri,
  uuid,
  sparqlEscapeString,
  sparqlEscapeDateTime,
  sparqlEscapeInt,
} from "mu";
import { updateSudo as update, querySudo as query } from "@lblod/mu-auth-sudo";
import { pipeline } from "stream/promises";
import zlib from "zlib";
import { PREFIXES, HIGH_LOAD_DATABASE_ENDPOINT } from "../constants";
import fs from "fs-extra";
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};

async function countFiles(task) {
  const queryStr = `
     ${PREFIXES}
       SELECT (count(distinct ?physicalFile) as ?count) {
           graph ?graph {
             ?task dct:isPartOf ${sparqlEscapeUri(task.job)};
             task:resultsContainer ?resultsContainer.
             ?resultsContainer task:hasFile ?logicalFile.
             ?logicalFile mu:uuid ?logicalFileUuid;  nfo:fileName ?logicalFileName.
             ?physicalFile nie:dataSource ?logicalFile;  nfo:fileName ?fileName; dct:format ?format.
             optional {?logicalFile prov:wasDerivedFrom ?derivedFrom}
           }
     }
`;
  const result = await query(queryStr, {}, connectionOptions);
  if (result.results.bindings.length) {
    return result.results.bindings[0].count.value;
  } else {
    return 0;
  }
}
export async function gzip(task) {
  const defaultLimitSize = 1000;
  const queryFn = async (limitSize, offset) => {
    const q = `
    ${PREFIXES}
    SELECT ?physicalFile ?logicalFile ?derivedFrom ?logicalFileUuid ?graph ?fileName ?logicalFileName ?format where {
      SELECT distinct ?physicalFile ?logicalFile ?derivedFrom ?logicalFileUuid ?graph ?fileName ?logicalFileName ?format
      WHERE {
        GRAPH ?graph {
             ?task dct:isPartOf ${sparqlEscapeUri(task.job)};
             task:resultsContainer ?resultsContainer.
             ?resultsContainer task:hasFile ?logicalFile.
             ?logicalFile mu:uuid ?logicalFileUuid;  nfo:fileName ?logicalFileName.
             ?physicalFile nie:dataSource ?logicalFile;  nfo:fileName ?fileName; dct:format ?format.
             optional {?logicalFile prov:wasDerivedFrom ?derivedFrom}
       }
      } order by ?physicalFile
    } limit ${limitSize} offset ${offset}`;
    const result = await query(q, {}, connectionOptions);
    return result.results.bindings || [];
  };
  const count = await countFiles(task);
  const pagesCount =
    count > defaultLimitSize ? Math.ceil(count / defaultLimitSize) : 1;

  for (let page = 0; page <= pagesCount; page++) {
    const response = await queryFn(defaultLimitSize, page * defaultLimitSize);
    for (const result of response) {
      // gzip the file and update triples
      const physicalFilePath = result.physicalFile.value.replace(
        "share://",
        "/share/",
      );

      if (!physicalFilePath.endsWith(".gz")) {
        const gzipPhyisicalFilePath = physicalFilePath + ".gz";
        await pipeline(
          fs.createReadStream(physicalFilePath),
          zlib.createGzip(),
          fs.createWriteStream(gzipPhyisicalFilePath),
        );
        const stats = await fs.stat(gzipPhyisicalFilePath);
        const fileSize = stats.size;
        const physicalFileUri = gzipPhyisicalFilePath.replace(
          "/share/",
          "share://",
        );
        const gzipFileName = result.fileName.value + ".gz";
        const phyId = uuid();
        const now = new Date();
        // prettier-ignore
        const updateStr = `
           ${PREFIXES}
           delete {
               graph ${sparqlEscapeUri(result.graph.value)} {
                 ${sparqlEscapeUri(result.physicalFile.value)} ?phyP ?phyO.
                 ${sparqlEscapeUri(result.logicalFile.value)} ?loP ?loO.
               }
           }
           insert {
               graph ${sparqlEscapeUri(result.graph.value)} {
                  ${sparqlEscapeUri(physicalFileUri)} a nfo:FileDataObject;
                                          nie:dataSource ${sparqlEscapeUri(result.logicalFile.value)} ;
                                          mu:uuid ${sparqlEscapeString(phyId)};
                                          nfo:fileName ${sparqlEscapeString(gzipFileName)} ;
                                          dct:creator <http://lblod.data.gift/services/job-compression-service>;
                                          dct:created ${sparqlEscapeDateTime(now)};
                                          dct:modified ${sparqlEscapeDateTime(now)};
                                          dct:format ${sparqlEscapeString(result.format.value)};
                                          nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                          dbpedia:fileExtension "gz".
                  ${sparqlEscapeUri(result.logicalFile.value)} a nfo:FileDataObject;
                                                               mu:uuid ${sparqlEscapeString(result.logicalFileUuid.value)};
                                          nfo:fileName ${sparqlEscapeString(result.logicalFileName.value + ".gz")} ;
                                          dct:creator <http://lblod.data.gift/services/job-compression-service>;
                                          ${result.derivedFrom?.value ? "prov:wasDerivedFrom " + sparqlEscapeUri(result.derivedFrom.value) + ";" : ""}
                                          dct:created ${sparqlEscapeDateTime(now)};
                                          dct:modified ${sparqlEscapeDateTime(now)};
                                          dct:format ${sparqlEscapeString(result.format.value)};
                                          nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                          dbpedia:fileExtension "gz" .
               }
           }
           where {
               graph ${sparqlEscapeUri(result.graph.value)} {
                 ${sparqlEscapeUri(result.physicalFile.value)} ?phyP ?phyO.
                 ${sparqlEscapeUri(result.logicalFile.value)} ?loP ?loO.
               }
           }

       `;
        await update(updateStr, {}, connectionOptions);
        fs.unlinkSync(physicalFilePath);
      }
    }
  }
}

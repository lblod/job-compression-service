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
import {
  PREFIXES,
  HIGH_LOAD_DATABASE_ENDPOINT,
  DEFAULT_GRAPH,
  STATUS_SUCCESS,
} from "../constants";
import { unlink, stat } from "fs/promises";
import { createReadStream, createWriteStream } from "fs";
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};

async function countFiles() {
  const queryStr = `
     ${PREFIXES}
       SELECT (count(distinct ?physicalFile) as ?count) {
           graph <${DEFAULT_GRAPH}> {
             ?task dct:isPartOf ?job;
             task:resultsContainer ?resultsContainer.
             ?job adms:status <${STATUS_SUCCESS}>.
             ?resultsContainer task:hasFile ?logicalFile.
             ?logicalFile mu:uuid ?logicalFileUuid;  nfo:fileName ?logicalFileName.
             ?physicalFile nie:dataSource ?logicalFile;  nfo:fileName ?fileName; dct:format ?format.
             ?logicalFile prov:wasDerivedFrom ?derivedFrom.
             FILTER NOT EXISTS {?physicalFile  dbpedia:fileExtension "gz"}
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
export async function gzip() {
  const defaultLimitSize = 100;
  const queryFn = async (limitSize, offset) => {
    const q = `
    ${PREFIXES}
    SELECT ?physicalFile ?logicalFile ?derivedFrom ?logicalFileUuid ?graph ?fileName ?logicalFileName ?format where {
      SELECT distinct ?physicalFile ?logicalFile ?derivedFrom ?logicalFileUuid ?graph ?fileName ?logicalFileName ?format
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
             ?task dct:isPartOf ?job;
             task:resultsContainer ?resultsContainer.
             ?job adms:status <${STATUS_SUCCESS}>.
             ?resultsContainer task:hasFile ?logicalFile.
             ?logicalFile mu:uuid ?logicalFileUuid;  nfo:fileName ?logicalFileName.
             ?physicalFile nie:dataSource ?logicalFile;  nfo:fileName ?fileName; dct:format ?format.
             ?logicalFile prov:wasDerivedFrom ?derivedFrom.
             FILTER NOT EXISTS {?physicalFile  dbpedia:fileExtension "gz"}
       }
      } order by ?physicalFile
    } limit ${limitSize} offset ${offset}`;
    const result = await query(q, {}, connectionOptions);
    return result.results.bindings || [];
  };
  const count = await countFiles();
  const pagesCount =
    count > defaultLimitSize ? Math.ceil(count / defaultLimitSize) : 1;

  for (let page = 0; page <= pagesCount; page++) {
    const response = await queryFn(defaultLimitSize, page * defaultLimitSize);

    await Promise.all(
      response.map(async (result) => {
        // gzip the file and update triples
        try {
          const physicalFilePath = result.physicalFile.value.replace(
            "share://",
            "/share/",
          );
          if (!physicalFilePath.endsWith(".gz")) {
            const gzipPhyisicalFilePath = physicalFilePath + ".gz";
            console.log(`gzipping ${physicalFilePath}...`);
            await pipeline(
              createReadStream(physicalFilePath, { encoding: "utf8" }),
              zlib.createGzip(),
              createWriteStream(gzipPhyisicalFilePath, { encoding: "utf8" }),
            );
            console.log(
              `gzipping ${gzipPhyisicalFilePath} done. collecting stats...`,
            );
            const stats = await stat(gzipPhyisicalFilePath);
            console.log(`stats for ${gzipPhyisicalFilePath} collected.`);
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
               graph ${sparqlEscapeUri(DEFAULT_GRAPH)} {
                 ${sparqlEscapeUri(result.physicalFile.value)} ?phyP ?phyO.
                 ${sparqlEscapeUri(result.logicalFile.value)} ?loP ?loO.
               }
           }
           insert {
               graph ${sparqlEscapeUri(DEFAULT_GRAPH)} {
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
                                          prov:wasDerivedFrom  ${sparqlEscapeUri(result.derivedFrom.value)};                                          
                                          dct:created ${sparqlEscapeDateTime(now)};
                                          dct:modified ${sparqlEscapeDateTime(now)};
                                          dct:format ${sparqlEscapeString(result.format.value)};
                                          nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                          dbpedia:fileExtension "gz" .
               }
           }
           where {
               graph ${sparqlEscapeUri(DEFAULT_GRAPH)} {
                 ${sparqlEscapeUri(result.physicalFile.value)} ?phyP ?phyO.
                 ${sparqlEscapeUri(result.logicalFile.value)} ?loP ?loO.
               }
           }

       `;
            await update(updateStr, {}, connectionOptions);
            await unlink(physicalFilePath);
          }
        } catch (e) {
          console.error("could not compress ", result, "error:", e);
        }
      }),
    );
  }
}

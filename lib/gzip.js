import { sparqlEscapeUri, uuid, sparqlEscapeString, sparqlEscapeInt } from 'mu';
import { updateSudo as update, querySudo as query } from '@lblod/mu-auth-sudo';
import { pipeline } from 'stream/promises';
import zlib from 'zlib';
import { PREFIXES } from '../constants';
import fs from 'fs-extra';


export async function gzip(task) {
  const queryStr = `
     ${PREFIXES}
     select distinct ?physicalFile ?logicalFile ?logicalFileUuid ?graph ?fileName ?logicalFileName ?format where {
           graph ?graph {
             ?task dct:isPartOf ${sparqlEscapeUri(task.job)};
             task:resultsContainer ?resultsContainer.
             ?resultsContainer task:hasFile ?logicalFile.
             ?logicalFile mu:uuid ?logicalFileUuid;  nfo:fileName ?logicalFileName.
             ?physicalFile nie:dataSource ?logicalFile;  nfo:fileName ?fileName; dct:format ?format.
           }
     }
`;
  const response = await query(queryStr);
  for (const result of response.results.bindings) {
    // gzip the file and update triples
    const physicalFilePath = result.physicalFile.value.replace('share://', '/share/');

    if (!physicalFilePath.endsWith(".gz")) {
      const gzipPhyisicalFilePath = physicalFilePath + ".gz";
      await pipeline(
        fs.createReadStream(physicalFilePath),
        zlib.createGzip(),
        fs.createWriteStream(gzipPhyisicalFilePath)
      );
      const stats = await fs.stat(gzipPhyisicalFilePath);
      const fileSize = stats.size;
      const physicalFileUri = gzipPhyisicalFilePath.replace('/share/', 'share://');
      const gzipFileName = result.fileName.value + ".gz";
      const phyId = uuid();
      const now = new Date();
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
      await update(updateStr);
      fs.unlinkSync(physicalFilePath);
    }
  }

}

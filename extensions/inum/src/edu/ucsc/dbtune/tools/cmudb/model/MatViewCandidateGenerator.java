package edu.ucsc.dbtune.tools.cmudb.model;

import Zql.ZQuery;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 17, 2008
 * Time: 2:46:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class MatViewCandidateGenerator {
    private List queries;

    public MatViewCandidateGenerator(List queries) {
        this.queries = queries;
    }

    public List generateMaterializedViews() {
        for (int i = 0; i < queries.size(); i++) {
            QueryDesc desc = (QueryDesc) queries.get(i);
            ZQuery zq = desc.parsed_query;
        }

        return null;
    }
}
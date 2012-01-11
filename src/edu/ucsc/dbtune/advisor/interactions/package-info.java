/**
 * This package implements the techniques developed in the paper "Index interactions in physical 
 * design tuning: modeling, analysis, and applications".
 * <p>
 * The degree of interaction of a pair of indexes {@latex.inline $a,b$} with respect to a query 
 * {@latex.inline $q$}, captures how strongly $a$ and $b$ interact in the processing of 
 * {@latex.inline $q$}, assuming that the hypothetical index-set {@latex.inline $X \\in S$} is 
 * materialized.
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
 */
package edu.ucsc.dbtune.advisor.interactions;

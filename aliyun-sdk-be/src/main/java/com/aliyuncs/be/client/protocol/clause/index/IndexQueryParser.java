package com.aliyuncs.be.client.protocol.clause.index;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
public class IndexQueryParser {
    private String expression;
    private List<String> segments = new ArrayList<>();

    public IndexQueryParser(String expression) {
        this.expression = expression;
    }

    public BeIndexQuery parse() throws InvalidParameterException {
        this.scan();
        Iterator<String> iterator = segments.iterator();
        return parseSubClause(iterator, false);
    }

    public String buildQueryClause(boolean encode) throws InvalidParameterException {
        String query = parse().buildCondition().toString();
        if (!encode) {
            return query;
        }
        try {
            return URLEncoder.encode(query, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException("Failed to encode query:" + query);
        }
    }

    public String buildQueryClause() throws InvalidParameterException {
        return buildQueryClause(true);
    }

    private void scan() {
        if (CollectionUtils.isNotEmpty(segments)) {
            segments = new ArrayList<>();
        }
        char current;
        int beginIndex = -1;
        segments.add("(");
        for (int i = 0; i < expression.length(); i++) {
            current = expression.charAt(i);
            switch (current) {
                case '(':
                case ')':
                case '=':
                    if (beginIndex >= 0) {
                        segments.add(expression.substring(beginIndex, i));
                        beginIndex = -1;
                    }
                    segments.add(String.valueOf(current));
                    break;
                case ' ':
                case '\t':
                    if (beginIndex >= 0) {
                        segments.add(expression.substring(beginIndex, i));
                        beginIndex = -1;
                    }
                    break;
                default:
                    if (beginIndex < 0) {
                        beginIndex = i;
                    }
            }
        }
        if (beginIndex >= 0) {
            segments.add(expression.substring(beginIndex));
        }
        segments.add(")");
    }

    private static class SubClause {
        List<IndexQueryOperator> operators = new ArrayList<>();
        List<BeIndexQuery> clauses = new ArrayList<>();

        BeIndexQuery buildClause() {
            if (clauses.size() == 1) {
                return clauses.get(0);
            }
            // Only AND
            if (!operators.contains(IndexQueryOperator.OR)) {
                return new CompoundQuery(IndexQueryOperator.AND, clauses);
            }
            // Only OR
            if (!operators.contains(IndexQueryOperator.AND)) {
                return new CompoundQuery(IndexQueryOperator.OR, clauses);
            }

            List<BeIndexQuery> orClauses = new ArrayList<>();
            CompoundQuery query = new CompoundQuery(IndexQueryOperator.OR, orClauses);
            int andBeginIndex = -1;
            for (int i = 0; i < operators.size(); i++) {
                if (IndexQueryOperator.AND.equals(operators.get(i))) {
                    if (andBeginIndex < 0) {
                        andBeginIndex = i;
                    }
                } else {
                    if (andBeginIndex >= 0) {
                        CompoundQuery lastAndClause = new CompoundQuery(IndexQueryOperator.AND);
                        for (int j = andBeginIndex; j <= i; j++) {
                            lastAndClause.addQuery(clauses.get(j));
                        }
                        orClauses.add(lastAndClause);
                        andBeginIndex = -1;
                    } else {
                        orClauses.add(clauses.get(i));
                    }
                }
            }

            if (andBeginIndex >= 0) {
                CompoundQuery lastAndClause = new CompoundQuery(IndexQueryOperator.AND);
                for (int j = andBeginIndex; j <= operators.size(); j++) {
                    lastAndClause.addQuery(clauses.get(j));
                }
                orClauses.add(lastAndClause);
            } else {
                orClauses.add(clauses.get(operators.size()));
            }
            return query;
        }
    }

    private BeIndexQuery parseSubClause(Iterator<String> iterator, boolean innerParser) throws InvalidParameterException {
        String currentLeft = null;
        boolean currentMatch = false;
        BeIndexQuery currentQuery = null;

        SubClause subClause = new SubClause();
        while(iterator.hasNext()) {
            String current = iterator.next();
            if (isClauseBegin(current)) {
                if (currentQuery != null) {
                    throw new InvalidParameterException("lack AND/OR operator between clauses");
                }
                currentQuery = parseSubClause(iterator, true);
            } else if (isClauseEnd(current)) {
                if (currentQuery == null) {
                    throw new InvalidParameterException("lack query clause before )");
                }
                subClause.clauses.add(currentQuery);
                return subClause.buildClause();
            } else if (isOrOperator(current) || isAndOperator(current)) {
                if (currentQuery == null) {
                    throw new InvalidParameterException(String.format("lack query clause before operator[%s]", current));
                }
                subClause.clauses.add(currentQuery);
                subClause.operators.add(IndexQueryOperator.parseOperator(current));

                // reset context
                currentLeft = null;
                currentMatch = false;
                currentQuery = null;
            } else if (isMatchOperator(current)) {
                if (currentMatch) {
                    throw new InvalidParameterException(
                            String.format("match operator already exist after indexName[%s]", currentLeft));
                }
                currentMatch = true;
            } else {
                if (currentQuery != null) {
                    throw new InvalidParameterException("lack AND/OR operator between clauses");
                }

                if (currentLeft == null) {
                    currentLeft = current;
                    continue;
                }
                if (!currentMatch) {
                    throw new InvalidParameterException(
                            String.format("lack match operator(=) between indexName[%s] and value[%s]", currentLeft, current));
                }
                currentQuery = new SimpleQuery(currentLeft, current);
            }
        }
        if (innerParser) {
            throw new InvalidParameterException("lack ) for sub clause");
        }
        subClause.clauses.add(currentQuery);
        return subClause.buildClause();
    }

    private boolean isClauseBegin(String segment) {
        return StringUtils.equals(segment, "(");
    }

    private boolean isClauseEnd(String segment) {
        return StringUtils.equals(segment, ")");
    }

    private boolean isMatchOperator(String segment) {
        return StringUtils.equals(segment, "=");
    }

    private boolean isAndOperator(String segment) {
        return StringUtils.equals(segment, "and");
    }

    private boolean isOrOperator(String segment) {
        return StringUtils.equals(segment, "or");
    }

}

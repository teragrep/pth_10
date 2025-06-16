/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth10.ast.commands.evalstatement.UDFs;

import com.google.gson.*;
import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.ast.QuotedText;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * UDF for command spath(json/xml, spath)<br>
 * <p>
 * First, the given (assumed to be) JSON/XML string is tried to be parsed as JSON, and if that fails, XML parsing is
 * attempted. Otherwise the function will return an empty result, or the original input if the input and output column
 * are set to the same column.
 * </p>
 * A separate 'xpath' command can be used for xpath expressions.
 *
 * @author eemhu
 */
public class Spath implements UDF4<String, String, String, String, Map<String, String>>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Spath.class);
    private static final long serialVersionUID = 1L;

    private final NullValue nullValue;

    public Spath(NullValue nullValue) {
        super();
        this.nullValue = nullValue;
    }

    /**
     * Returns result of spath as a map Keys are wrapped in backticks to escape dots, spark uses them for maps
     * 
     * @param input           json/xml input
     * @param spathExpr       spath/xpath expression
     * @param nameOfInputCol  name of input column
     * @param nameOfOutputCol name of output column
     * @return map of results
     * @throws Exception
     */
    @Override
    public Map<String, String> call(String input, String spathExpr, String nameOfInputCol, String nameOfOutputCol)
            throws Exception {
        // Map to return at the end of this function
        final Map<String, String> result = new HashMap<>();
        try {
            // try json
            final Gson gson = new Gson();
            final JsonElement jsonElem = gson.fromJson(input, JsonElement.class);

            // Auto-extraction (JSON)
            if (spathExpr == null) {
                // expect topmost element to be an object
                for (Map.Entry<String, JsonElement> sub : jsonElem.getAsJsonObject().entrySet()) {
                    // put key:value to map - unescaping result in case was a nested json string
                    result
                            .put(new QuotedText(new TextString(sub.getKey()), "`").read(), new UnquotedText(new TextString(StringEscapeUtils.unescapeJson(sub.getValue().toString()))).read());
                }
            }
            // Manual extraction via spath expression (JSON)
            else {
                final JsonElement jsonSubElem = getJsonElement(
                        jsonElem, new UnquotedText(new TextString(spathExpr)).read()
                );
                // put key:value to map - unescaping result in case was a nested json string
                result
                        .put(new QuotedText(new TextString(spathExpr), "`").read(), jsonSubElem != null ? new UnquotedText(new TextString(StringEscapeUtils.unescapeJson(jsonSubElem.toString()))).read() : nullValue.value());
            }
            return result;
        }
        catch (JsonSyntaxException | ClassCastException json_fail) {
            LOGGER.warn("Processing failed as JSON, trying XML parsing. Error: <{}>", json_fail.getMessage());
            // try xml
            try {
                Document doc = getXmlDocFromString(input);

                if (doc == null) {
                    // failed to make document from string
                    return result;
                }

                // Auto-extraction (XML)
                if (spathExpr == null) {
                    // Each tag-pair containing text inside will be given a new column
                    // main-sub-item would contain all for that type of nested tags, etc.
                    final Node rootNode = doc.getDocumentElement();
                    buildMapFromXmlNodes(rootNode, ".", result);
                }
                // Manual extraction via spath expression (XML)
                else {
                    // spath expects spath at all times, even when input is XML
                    // spath needs to be converted to xpath for xml
                    final XPath xPath = XPathFactory.newInstance().newXPath();

                    // spath is of type main.sub.item, convert to /main/sub/item
                    String spathAsXpath = "/"
                            .concat(new UnquotedText(new TextString(spathExpr)).read())
                            .replaceAll("\\.", "/");
                    LOGGER.debug("spath->xpath conversion: <[{}]>", spathAsXpath);

                    String rv = (String) xPath.compile(spathAsXpath).evaluate(doc, XPathConstants.STRING);
                    result.put(new QuotedText(new TextString(spathExpr), "`").read(), rv.trim());
                }
                return result;
            }
            catch (Exception e) {
                LOGGER.warn("spath: The content couldn't be parsed as JSON or XML. Details: <{}>", e.getMessage());
                // return pre-existing content if output is the same as input
                if (nameOfInputCol.equals(nameOfOutputCol)) {
                    result.put(new QuotedText(new TextString(spathExpr), "`").read(), input);
                }
                // otherwise output will be empty on error
                else {
                    result.put(new QuotedText(new TextString(spathExpr), "`").read(), nullValue.value());
                }
                return result;
            }

        }
    }

    /**
     * Gets JSON element from JSON based on the given SPath expression
     * 
     * @param json  JSONElement to get the (sub)element from
     * @param spath SPath expression which expresses the element to get
     */
    private JsonElement getJsonElement(final JsonElement json, final String spath) {
        final String[] parts = spath.split("[.\\[\\]]");
        JsonElement rv = json;

        for (String key : parts) {
            key = key.trim();

            LOGGER.debug("Got key: <{}>", key);

            if (key.isEmpty()) {
                LOGGER.debug("Key was empty");
                continue;
            }

            if (rv == null || rv.isJsonNull()) {
                LOGGER.debug("Given JsonElement was a NULL");
                rv = JsonNull.INSTANCE;
                break;
            }

            if (rv.isJsonObject()) {
                LOGGER.debug("Given JsonElement was an OBJECT");
                rv = ((JsonObject) rv).get(key);
            }
            else if (rv.isJsonArray()) {
                LOGGER.debug("Given JsonElement was an ARRAY");
                int i = Integer.parseInt(key) - 1;
                rv = ((JsonArray) rv).get(i);
            }
            else {
                LOGGER.debug("Given JsonElement was something else");
                break;
            }
        }

        return rv;
    }

    /**
     * Convert an XML-formatted string to a Document object
     * 
     * @param xmlStr XML-formatted string
     * @return (XML) Document object
     */
    private Document getXmlDocFromString(final String xmlStr) {
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();

        DocumentBuilder docBuilder;

        try {
            docBuilder = docBuilderFactory.newDocumentBuilder();
            return docBuilder.parse(new InputSource(new StringReader(xmlStr)));
        }
        catch (Exception e) {
            LOGGER.warn("Failed to parse XML: <{}>", e);
            return null;
        }
    }

    /**
     * Adds all 'node tag - contents' Key-Value pairs to the map <pre>tag.tag.tag => contents</pre>
     * 
     * @param rootNode root node (Main Document Element)
     * @param spacer   Spacer string between each parent->child in key name
     * @param map      Final map to be returned out of the UDF
     */
    private void buildMapFromXmlNodes(final Node rootNode, final String spacer, final Map<String, String> map) {
        // RootNode is text
        if (rootNode.getNodeName().equals("#text")) {
            String colName = "";

            Node parent = rootNode.getParentNode();
            boolean isFirst = true;

            // add each parent of parent as the top-most label
            // grandparent.parent.child
            // loop goes from child -> parent -> grandparent
            while (parent != null) {
                // top-most parent is #document, not needed
                if (!parent.getNodeName().equals("#document")) {
                    colName = parent.getNodeName().concat(isFirst ? "" : spacer).concat(colName);
                }

                // get parent of parent
                parent = parent.getParentNode();
                isFirst = false;
            }

            // if there are multiple columns of the same name, add value to existing column
            final String quotedColName = new QuotedText(new TextString(colName), "`").read();
            if (map.containsKey(quotedColName)) {
                map
                        .computeIfPresent(quotedColName, (k, existingValue) -> existingValue.concat("\n").concat(rootNode.getTextContent()));
            }
            else {
                map.put(quotedColName, rootNode.getTextContent());
            }
        }

        final NodeList nl = rootNode.getChildNodes();
        // Visit children of current node
        for (int i = 0; i < nl.getLength(); i++) {
            Node child = nl.item(i);
            buildMapFromXmlNodes(child, spacer, map);
        }
    }
}

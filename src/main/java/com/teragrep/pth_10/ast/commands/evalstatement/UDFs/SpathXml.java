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
package com.teragrep.pth_10.ast.commands.evalstatement.UDFs;

import com.teragrep.pth_10.ast.NullValue;
import com.teragrep.pth_10.ast.QuotedText;
import com.teragrep.pth_10.ast.TextString;
import com.teragrep.pth_10.ast.UnquotedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public final class SpathXml {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpathXml.class);
    final String input;
    final String spathExpression;
    final String inputColumn;
    final String outputColumn;
    final NullValue nullValue;

    /**
     * Returns result of spath as a map Keys wrapped in backticks to escape dots, spark uses them for maps
     *
     * @param input           xml input
     * @param spathExpression xpath expression
     * @param inputColumn     name of input column
     * @param outputColumn    name of output column
     */
    public SpathXml(final String input, final String spathExpression, final String inputColumn, final String outputColumn) {
        this(input, spathExpression, inputColumn, outputColumn, new NullValue());
    }

    private SpathXml(
            final String input,
            final String spathExpression,
            final String inputColumn,
            final String outputColumn,
            final NullValue nullValue
    ) {
        this.input = input;
        this.spathExpression = spathExpression;
        this.inputColumn = inputColumn;
        this.outputColumn = outputColumn;
        this.nullValue = nullValue;
    }

    public Map<String, String> asMap() {
        final Map<String, String> result = new HashMap<>();
        final Document doc;

        try {
            doc = getXmlDocFromString(input);
        }
        catch (IOException | SAXException | ParserConfigurationException xml_fail) {
            LOGGER.warn("Processing failed as XML parsing. Error: <{}>", xml_fail.getMessage());
            return result;
        }
        try {
            // Auto-extraction (XML)
            if (spathExpression == null) {
                // Each tag-pair containing text inside will be given a new column
                // main-sub-item would contain all for that type of nested tags, etc.
                final Node rootNode = doc.getDocumentElement();
                buildMapFromXmlNodes(rootNode, ".", result);
            }
            // Manual extraction via spath expression (XML)
            else {
                // convert spath to xpath for xml
                final XPath xPath = XPathFactory.newInstance().newXPath();
                // spath is of type main.sub.item, convert to /main/sub/item
                final String spathAsXpath = "/"
                        .concat(new UnquotedText(new TextString(spathExpression)).read())
                        .replaceAll("\\.", "/");
                LOGGER.debug("spath->xpath conversion: <[{}]>", spathAsXpath);
                final String rv = String.valueOf(xPath.compile(spathAsXpath).evaluate(doc, XPathConstants.STRING));
                result.put(new QuotedText(new TextString(spathExpression), "`").read(), rv.trim());
            }
            return result;
        }
        catch (Exception e) {
            LOGGER.warn("spath: The content couldn't be parsed as JSON or XML. Details: <{}>", e.getMessage());
            // return pre-existing content if output is the same as input
            if (inputColumn.equals(outputColumn)) {
                result.put(new QuotedText(new TextString(spathExpression), "`").read(), input);
            }
            // otherwise output will be empty on error
            else {
                result.put(new QuotedText(new TextString(spathExpression), "`").read(), nullValue.value());
            }
            return result;
        }
    }

    /**
     * Convert an XML-formatted string to a Document object
     *
     * @param xmlStr XML-formatted string
     * @return (XML) Document object
     */
    private Document getXmlDocFromString(final String xmlStr)
            throws IOException, SAXException, ParserConfigurationException {
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        return docBuilder.parse(new InputSource(new StringReader(xmlStr)));
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

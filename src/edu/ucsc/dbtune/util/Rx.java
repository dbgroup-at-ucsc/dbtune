package edu.ucsc.dbtune.util;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author Rui Wang
 */
public class Rx {
    private static Object sync = new Object();
    private static DocumentBuilder builder;
    private static Transformer xformer;

    public static void allowAllCharacters() {
        try {
            Class class1 = Class
                    .forName("com.sun.org.apache.xerces.internal.util.XMLChar");
            Field field = class1.getDeclaredField("CHARS");
            field.setAccessible(true);
            byte[] CHARS = (byte[]) field.get(null);
            CHARS[0] |= 0x1;
            CHARS[2] |= 0x1;
            CHARS[3] |= 0x1;
            CHARS[4] |= 0x1;
            CHARS[5] |= 0x1;
            CHARS[6] |= 0x1;
            CHARS[7] |= 0x1;
            CHARS[8] |= 0x1;
            CHARS[11] |= 0x1;
            CHARS[12] |= 0x1;
            CHARS[14] |= 0x1;
            CHARS[16] |= 0x1;
            CHARS[17] |= 0x1;
            CHARS[18] |= 0x1;
            CHARS[19] |= 0x1;
            CHARS[20] |= 0x1;
            CHARS[21] |= 0x1;
            CHARS[22] |= 0x1;
            CHARS[24] |= 0x1;
            CHARS[25] |= 0x1;
            CHARS[26] |= 0x1;
            CHARS[27] |= 0x1;
            CHARS[28] |= 0x1;
            CHARS[29] |= 0x1;
            CHARS[30] |= 0x1;
            CHARS[31] |= 0x1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void checkBuilder() {
        synchronized (sync) {
            if (builder == null) {
                DocumentBuilderFactory factory = DocumentBuilderFactory
                        .newInstance();
                try {
                    builder = factory.newDocumentBuilder();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getXml(Rx root, boolean format)
            throws TransformerException {
        synchronized (sync) {
            if (xformer == null) {
                try {
                    xformer = TransformerFactory.newInstance().newTransformer();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        synchronized (xformer) {
            if (format)
                Rx.formatXmlNode(root.document, root.element, 0);
            Source source = new DOMSource(root.element);
            java.io.StringWriter stringWriter = new StringWriter();
            // java.io.ByteArrayOutputStream outputStream = new
            // ByteArrayOutputStream();
            Result result = new StreamResult(stringWriter);
            xformer.transform(source, result);
            return stringWriter.toString();
        }
    }

    private static void formatXmlNode(org.w3c.dom.Document document,
            Element element, int level) {
        ArrayList<Node> keepedNode = new ArrayList<Node>();
        boolean hasChild = false;

        char[] spaces = new char[level * 2];
        Arrays.fill(spaces, ' ');
        String space = new String(spaces);
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                if (!hasChild) {
                    keepedNode.add(document.createTextNode("\n"));
                    hasChild = true;
                }
                keepedNode.add(document.createTextNode(space + "  "));
                keepedNode.add(node);
                keepedNode.add(document.createTextNode("\n"));
                formatXmlNode(document, (Element) node, level + 1);
            } else if (node.getTextContent().trim().length() > 0) {
                keepedNode.add(node);
            }
        }
        for (int i = element.getChildNodes().getLength() - 1; i >= 0; i--) {
            element.removeChild(element.getChildNodes().item(i));
        }
        if (hasChild)
            keepedNode.add(document.createTextNode(space));
        for (Node node : keepedNode) {
            element.appendChild(node);
        }
    }

    private static Element _findChildNode(Node parentNode, String name) {
        for (Node node = parentNode.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                if (((Element) node).getTagName().equals(name))
                    return (Element) node;
            }
        }
        return null;
    }

    private static Element _createChildNode(org.w3c.dom.Document document,
            Node parentNode, String name) {
        Element element = document.createElement(name);
        parentNode.appendChild(element);
        return element;
    }

    private static Element _createChildNode(org.w3c.dom.Document document,
            Node parentNode, String name, String text) {
        Element element = document.createElement(name);
        element.appendChild(document.createTextNode(text));
        parentNode.appendChild(element);
        return element;
    }

    private static Element _findOrCreateChildNode(
            org.w3c.dom.Document document, Node parentNode, String name) {
        Element node = _findChildNode(parentNode, name);

        if (node == null)
            node = _createChildNode(document, parentNode, name);
        return node;
    }

    private static void _clearXmlNode(Node node) {
        for (int i = node.getChildNodes().getLength() - 1; i >= 0; i--) {
            node.removeChild(node.getChildNodes().item(i));
        }
    }

    private static void _formatXmlTree(org.w3c.dom.Document document) {
        for (Node node = document.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                formatXmlNode(document, (Element) node, 0);
            }
        }
    }

    private final Document document;
    private Rx parent;
    private final Element element;

    public Rx(String name) {
        checkBuilder();
        synchronized (builder) {
            this.document = builder.newDocument();
        }
        this.element = document.createElement(name);
    }

    public static Rx findRoot(String xml, String rootName) throws SAXException,
            IOException {
        checkBuilder();
        synchronized (builder) {
            Document xmldocument = builder.parse(new InputSource(
                    new java.io.StringReader(xml)));
            Element element = null;
            for (Node node = xmldocument.getFirstChild(); node != null; node = node
                    .getNextSibling()) {
                if (node instanceof Element
                        && ((Element) node).getTagName().equals(rootName)) {
                    return new Rx(xmldocument, null, (Element) node);
                }
            }
            return null;
        }
    }

    public static Rx findRoot(String xml) throws SAXException, IOException {
        checkBuilder();
        synchronized (builder) {
            Document xmldocument = builder.parse(new InputSource(
                    new java.io.StringReader(xml)));
            Element element = null;
            for (Node node = xmldocument.getFirstChild(); node != null; node = node
                    .getNextSibling()) {
                if (node instanceof Element) {
                    return new Rx(xmldocument, null, (Element) node);
                }
            }
            return null;
        }
    }

    private Rx(Document document, Rx parent, Element element) {
        this.document = document;
        this.parent = parent;
        this.element = element;
    }

    public Rx findChild(String name) {
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                if (((Element) node).getTagName().equals(name))
                    return new Rx(document, this, ((Element) node));
            }
        }
        return null;
    }

    public Rx findChildWithAttribute(String name, String attrName,
            String attrValue) {
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                Element element = (Element) node;
                if (element.getTagName().equals(name)) {
                    String v = element.getAttribute(attrName);
                    if (attrValue.equals(v))
                        return new Rx(document, this, ((Element) node));
                }
            }
        }
        return null;
    }

    public String getChildText(String name) {
        Rx child = findChild(name);
        if (child == null)
            return null;
        return child.getText();
    }

    public String getChildXmlText(String name) {
        Rx child = findChild(name);
        if (child == null)
            return null;
        return child.getXmlText();
    }

    public int getChildIntContent(String name) {
        Rx child = findChild(name);
        if (child == null)
            return 0;
        String string = child.getText().trim();
        if (string.length() == 0)
            return 0;
        return Integer.parseInt(string);
    }

    public long getChildLongContent(String name) {
        Rx child = findChild(name);
        if (child == null)
            return 0;
        String string = child.getText().trim();
        if (string.length() == 0)
            return 0;
        return Long.parseLong(string);
    }

    public float getChildFloatContent(String name, float def) {
        Rx child = findChild(name);
        if (child == null)
            return def;
        String string = child.getText().trim();
        if (string.length() == 0)
            return def;
        return Float.parseFloat(string);
    }

    public double getChildDoubleContent(String name) {
        Rx child = findChild(name);
        if (child == null)
            return 0;
        String string = child.getText().trim();
        if (string.length() == 0)
            return 0;
        return Double.parseDouble(string);
    }

    public boolean getChildBooleanContent(String name, boolean def) {
        Rx child = findChild(name);
        if (child == null)
            return def;
        String string = child.getText().trim();
        if (string.length() == 0)
            return def;
        return Boolean.parseBoolean(string);
    }

    public Rx[] findChilds(String name) {
        java.util.Vector<Rx> vector = new Vector<Rx>();
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                if (((Element) node).getTagName().equals(name))
                    vector.add(new Rx(document, this, ((Element) node)));
            }
        }
        return vector.toArray(new Rx[vector.size()]);
    }

    public Rx[] findChilds() {
        java.util.Vector<Rx> vector = new Vector<Rx>();
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                vector.add(new Rx(document, this, ((Element) node)));
            }
        }
        return vector.toArray(new Rx[vector.size()]);
    }

    public Rx createChild(String name) {
        Element childElement = document.createElement(name);
        element.appendChild(childElement);
        return new Rx(document, this, childElement);
    }

    public Rx createChild(String name, String text) {
        if (text == null)
            text = "";
        Element childElement = document.createElement(name);
        childElement.appendChild(document.createTextNode(text));
        element.appendChild(childElement);
        return new Rx(document, this, childElement);
    }

    public void setText(String text) {
        element.setTextContent(text);
    }

    public Rx createChild(String name, int value) {
        return createChild(name, Integer.toString(value));
    }

    public Rx createChild(String name, long value) {
        return createChild(name, Long.toString(value));
    }

    public Rx createChild(String name, double value) {
        return createChild(name, Double.toString(value));
    }

    public Rx createChild(String name, boolean value) {
        return createChild(name, "" + value);
    }

    public String getName() {
        return element.getTagName();
    }

    public String getText() {
        return element.getTextContent();
    }

    private void getXmlText(Node node, StringBuilder sb) {
        for (Node sub = node.getFirstChild(); sub != null; sub = sub
                .getNextSibling()) {
            if (sub.getNodeType() == Node.TEXT_NODE) {
                sb.append(sub.getTextContent());
            } else if (sub.getNodeType() == Node.ELEMENT_NODE) {
                Element e = (Element) sub;
                sb.append("<" + sub.getNodeName());
                NamedNodeMap attrs = e.getAttributes();
                if (attrs.getLength() > 0) {
                    for (int i = 0; i < attrs.getLength(); i++) {
                        Node n = attrs.item(i);
                        sb.append(" " + n.getNodeName() + "=\""
                                + n.getNodeValue() + "\"");
                    }
                }
                sb.append(">");
                getXmlText(sub, sb);
                sb.append("</" + sub.getNodeName() + ">");
            }
        }
    }

    public String getXmlText() {
        StringBuilder sb = new StringBuilder();
        getXmlText(element, sb);
        return sb.toString();
    }

    public String loadChildStringProperty(String key, String defaultValue) {
        Rx element = findChild(key);
        if (element != null)
            return element.getText();
        else
            return defaultValue;
    }

    public int loadChildIntProperty(String key, int defaultValue) {
        try {
            return Integer.parseInt(loadChildStringProperty(key, ""
                    + defaultValue));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long loadChildLongProperty(String key, long defaultValue) {
        try {
            return Long.parseLong(loadChildStringProperty(key, ""
                    + defaultValue));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean loadChildBooleanProperty(String key, boolean defaultValue) {
        return Boolean.parseBoolean(loadChildStringProperty(key, ""
                + defaultValue));
    }

    public void setNodeValue(String nodeValue) {
        element.setNodeValue(nodeValue);
    }

    public void setAttribute(String name, String value) {
        element.setAttribute(name, value);
    }

    public void removeAttribute(String name) {
        element.removeAttribute(name);
    }

    public void setAttribute(String name, int value) {
        element.setAttribute(name, "" + value);
    }

    public void setAttribute(String name, long value) {
        element.setAttribute(name, "" + value);
    }

    public void setAttribute(String name, boolean value) {
        element.setAttribute(name, "" + value);
    }

    public void setAttribute(String name, double value) {
        element.setAttribute(name, "" + value);
    }

    public String getAttribute(String name) {
        return getAttribute(name, null);
    }

    public String getAttribute(String name, String def) {
        String value = element.getAttribute(name);
        if (value == null || value.length() == 0)
            return def;
        return value;
    }

    public NamedNodeMap getAttributes() {
        return element.getAttributes();
    }

    public boolean getBooleanAttribute(String name) {
        String string = getAttribute(name);
        if (string == null)
            return false;
        return Boolean.parseBoolean(string);
    }

    public int getIntAttribute(String name) {
        return getIntAttribute(name, 0);
    }

    public int getIntAttribute(String name, int def) {
        String string = getAttribute(name);
        if (string == null)
            return def;
        return Integer.parseInt(string);
    }

    public long getLongAttribute(String name) {
        String string = getAttribute(name);
        if (string == null)
            return 0;
        return Long.parseLong(string);
    }

    public float getFloatAttribute(String name) {
        String string = getAttribute(name);
        if (string == null || string.trim().length() == 0)
            return 0;
        return Float.parseFloat(string);
    }

    public double getDoubleAttribute(String name) {
        String string = getAttribute(name);
        if (string == null)
            return 0;
        return Double.parseDouble(string);
    }

    public Rx getFirstChild() {
        for (Node node = element.getFirstChild(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                return new Rx(document, this, ((Element) node));
            }
        }
        return null;
    }

    public Rx getNextSibling() {
        for (Node node = element.getNextSibling(); node != null; node = node
                .getNextSibling()) {
            if (node instanceof Element) {
                return new Rx(document, this.parent, ((Element) node));
            }
        }
        return null;
    }

    public String[] selectXPath(String xpath) throws Exception {
        if (xpath.startsWith("/"))
            xpath = xpath.substring(1);
        String[] xpaths = xpath.split("/");
        Vector<String> values = new Vector<String>();
        if (!xpaths[0].equals(this.getName()))
            throw new Exception("xpath error");
        selectXPath(values, xpaths, 1);
        return values.toArray(new String[values.size()]);
    }

    private void selectXPath(Vector<String> values, String[] xpath, int pos) {
        String pattern = xpath[pos];
        if (pos < xpath.length - 1) {
            if (pattern.startsWith("@") || pattern.equals("text()"))
                throw new Error("Bad xpath");
            Rx[] childs = findChilds(pattern);
            for (Rx child : childs) {
                child.selectXPath(values, xpath, pos + 1);
            }
        } else {
            if (pattern.startsWith("@")) {
                pattern = pattern.substring(1);
                String value = getAttribute(pattern);
                if (value != null)
                    values.add(value);
                return;
            } else if (pattern.equals("text()")) {
                values.add(this.getText());
            } else {
                throw new Error("Xpath error");
            }
        }
    }

    public String getXml() throws TransformerException {
        return getXml(true);
    }

    public String getXml(boolean format) throws TransformerException {
        return Rx.getXml(this, format);
    }

    public String getCleanXml() throws TransformerException {
        String xml = getXml();
        if (xml.startsWith("<?")) {
            int t = xml.indexOf("?>");
            if (t > 0)
                xml = xml.substring(t + 2);
        }
        return xml;
    }

    public String toString() {
        try {
            return Rx.getXml(this, true);
        } catch (TransformerException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getXPath() {
        StringBuilder sb = new StringBuilder();
        for (Rx node = this; node != null; node = node.parent) {
            sb.insert(0, "/" + node.getName());
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        // String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ArticleList
        // nextLink=\
        // "http://resource.stockstar.com/Stock/colnews.asp?id=2463&amp;n=61\">\r\n"
        // + " <Article date=\"2007-07-28 12:27\"
        // link=\
        // "http://news.stockstar.com/info/darticle.aspx?id=GA,20070728,00593247&amp;columnid=2463\">abc</Article>\r\n"
        // + "</ArticleList>";
        // RXNode root = RXNode.findRoot(xml, "ArticleList");
        // RXNode[] element = root.findChilds("Article");
        // System.out.println(element[0].getAttribute("link"));
        // Rx root = new Rx("abc");
        // root.setAttribute("a", "b");
        // Rx r2=root.createChild("a");
        // r2.setAttribute("href", "www.abc.com");
        // r2.setText("abc");
        Rx root = Rx.findRoot("<a>abc<a href=\"abc.com\">bcd</a></a>");
        System.out.println(root.getXmlText());
    }
}

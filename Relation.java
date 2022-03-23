import java.io.Serializable;

public class Relation implements Serializable {
    private String RelationName;
    private String AttributeName;
    private Integer AttributeValue;
    Relation(String RelationName, String AttributeName, Integer AttributeValue) {
        this.RelationName = RelationName;
        this.AttributeName = AttributeName;
        this.AttributeValue = AttributeValue;
    }
    Relation(){

    }

    public String getAttributeName() {
        return AttributeName;
    }

    public String getRelationName() {
        return RelationName;
    }

    public Integer getAttributeValue() {
        return AttributeValue;
    }

    public void setAttributeName(String attributeName) {
        AttributeName = attributeName;
    }

    public void setRelationName(String relationName) {
        RelationName = relationName;
    }

    public void setAttributeValue(Integer attributeValue) {
        AttributeValue = attributeValue;
    }
    //    private
}

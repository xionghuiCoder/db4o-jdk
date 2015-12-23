package com.jd.o2o.db4o.core;

import java.lang.reflect.Method;

import com.db4o.query.Constraint;
import com.jd.o2o.db4o.exception.Db4oJdkException;

/**
 * Constraint代理类
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 下午7:11:03
 */
public class ConstraintProxy implements Constraint {
  private final Constraint constraint;

  private final QueryProxy queryProxy;

  private static final Method AND_METHOD;
  private static final Method OR_METHOD;
  private static final Method EQUAL_METHOD;
  private static final Method GREATER_METHOD;
  private static final Method SMALLER_METHOD;
  private static final Method IDENTITY_METHOD;
  private static final Method BYEXAMPLE_METHOD;
  private static final Method LIKE_METHOD;
  private static final Method CONTAINS_METHOD;
  private static final Method STARTSWITH_METHOD;
  private static final Method ENDSWITH_METHOD;
  private static final Method NOT_METHOD;

  static {
    try {
      AND_METHOD = Constraint.class.getMethod("and", Constraint.class);
      OR_METHOD = Constraint.class.getMethod("or", Constraint.class);
      EQUAL_METHOD = Constraint.class.getMethod("equal");
      GREATER_METHOD = Constraint.class.getMethod("greater");
      SMALLER_METHOD = Constraint.class.getMethod("smaller");
      IDENTITY_METHOD = Constraint.class.getMethod("identity");
      BYEXAMPLE_METHOD = Constraint.class.getMethod("byExample");
      LIKE_METHOD = Constraint.class.getMethod("like");
      CONTAINS_METHOD = Constraint.class.getMethod("contains");
      STARTSWITH_METHOD = Constraint.class.getMethod("startsWith", boolean.class);
      ENDSWITH_METHOD = Constraint.class.getMethod("endsWith", boolean.class);
      NOT_METHOD = Constraint.class.getMethod("not");
    } catch (Exception e) {
      throw new Db4oJdkException(e);
    }
  }

  public ConstraintProxy(Constraint constraint, QueryProxy queryProxy) {
    this.constraint = constraint;
    queryProxy.addInvokeObj(constraint);
    this.queryProxy = queryProxy;
  }

  @Override
  public Constraint and(Constraint with) {
    queryProxy.trackOperation(constraint, AND_METHOD, with);
    Constraint andConstraint = constraint.and(with);
    if (andConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(andConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint or(Constraint with) {
    queryProxy.trackOperation(constraint, OR_METHOD, with);
    Constraint orConstraint = constraint.or(with);
    if (orConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(orConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint equal() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, EQUAL_METHOD, args);
    Constraint equalConstraint = constraint.equal();
    if (equalConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(equalConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint greater() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, GREATER_METHOD, args);
    Constraint greaterConstraint = constraint.greater();
    if (greaterConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(greaterConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint smaller() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, SMALLER_METHOD, args);
    Constraint smallerConstraint = constraint.smaller();
    if (smallerConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(smallerConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint identity() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, IDENTITY_METHOD, args);
    Constraint identityConstraint = constraint.identity();
    if (identityConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(identityConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint byExample() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, BYEXAMPLE_METHOD, args);
    Constraint byExampleConstraint = constraint.byExample();
    if (byExampleConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(byExampleConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint like() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, LIKE_METHOD, args);
    Constraint likeConstraint = constraint.like();
    if (likeConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(likeConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint contains() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, CONTAINS_METHOD, args);
    Constraint containsConstraint = constraint.contains();
    if (containsConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(containsConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint startsWith(boolean caseSensitive) {
    queryProxy.trackOperation(constraint, STARTSWITH_METHOD, caseSensitive);
    Constraint startsWithConstraint = constraint.startsWith(caseSensitive);
    if (startsWithConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(startsWithConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint endsWith(boolean caseSensitive) {
    queryProxy.trackOperation(constraint, ENDSWITH_METHOD, caseSensitive);
    Constraint endsWithConstraint = constraint.endsWith(caseSensitive);
    if (endsWithConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(endsWithConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Constraint not() {
    Object[] args = null;
    queryProxy.trackOperation(constraint, NOT_METHOD, args);
    constraint.not();
    Constraint notConstraint = constraint.not();
    if (notConstraint.equals(constraint)) {
      return this;
    }
    ConstraintProxy constraintProxy = new ConstraintProxy(notConstraint, queryProxy);
    return constraintProxy;
  }

  @Override
  public Object getObject() {
    return constraint.getObject();
  }
}

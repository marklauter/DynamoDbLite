# Kingo
## solution description
The solution, called Kingo, is a Google Zanzibar style ReBAC system.
Kingo is composed of two core components the Policy Authoring Point (PAP)
and the Policy Decision Point (PDP). Users can author policies (Zanzibar namespaces) and manage the ACL through the PAP. 
Policies are defined with a custom policy description language (PDL).
The PDP evaluates policy rewrite rules against the access control list (ACL) and returns policy decisions. 
The core function of the PDP is `IsMemberOf(subject, subject-set)`.
PDP decisions are recorded in the descision journal.

The storage system is an MVCC key-value store backed by SQLite. Each record is split into a mutable header and an append-only journal, providing a complete audit history.
A key performance strategy, inspired by Zanzibar, is the use of dictionary encoding to map string identifiers (namespaces, relations, subjects) to integers, which can be packed into a 64-bit unsigned integer for fast lookups.

## core concepts
- **ACL Tuples**: Access control lists are stored as tuples in the format `(Subject, Relation, Object)`.
- **Policy Definition Language (PDL)**: A custom language for defining namespaces, relations, and subject-set rewrite rules. The full BNF and examples are in `README.md`.
- **SubjectSet Rewrite**: The core mechanism for checking permissions. It involves recursively expanding relations according to the rules defined in the PDL to determine if a subject is a member of a set.
- **Dictionary Encoding**: A performance optimization where string identifiers for namespaces, relations, and subjects are mapped to integer values. This allows ACL tuples to be stored and looked up efficiently.

## key projects
- **Kingo**: The root project containing primitive types and core domain models.
- **Kingo.Json**: Contains custom `JsonConverter` classes for serializing and deserializing Kingo's primitive types.
- **Kingo.Storage**: Implements the MVCC key-value store using SQLite, including the header/journal table structure.
- **Kingo.Policies**: Contains the Policy Definition Language (PDL) tokenizer, parser, and Abstract Syntax Tree (AST) classes.

## policy specs / subjectset rewrite rules
policy description language (PDL) for building namespaces, relationships, and rewrite expressions

PDL BNF
```bnf
# operator precedence: ! (exclude), &|= (intersect/union same level)
# associative operators: & (intersection), | (union) - same precedence
# non-associative operator: ! (exclusion) - highest precedence
# expressions
<policy-set>    ::= <namespace-map>
<namespace-map> ::= <namespace> [ <namespace> ]*
<namespace>     ::= <identifier>: <relation-seq>
<relation-seq>  ::= - <relation>
<relation>      ::= <relation-identifier>
                  | <relation-identifier>: <rewrite>

<rewrite>       ::= <exclusion> [ ('&' | '|') <exclusion> ]*
<exclusion>     ::= <term> [ '!' <term> ]*
<term>          ::= 'this'
                  | <computed-subjectset>
                  | <tuple-to-subjectset>
                  | '(' <rewrite> ')'

# terms
<computed-subjectset>   ::= <identifier>
<tuple-to-subjectset>   ::= '(' <identifier> ',' <identifier> ')'
<relation-identifier>   ::= <identifier>
<identifier>            ::= [a-zA-Z_][a-zA-Z0-9_]*

<comment>       ::= '#' [^<newline>]*
<newline>       ::= '\n' | '\r\n'
```

PDL sample:
```yaml
# rewrite set operators:
#   ! = exclusion operator
#   & = intersection operator
#   | = union operator

file:                           # namespace
  - owner                       # empty relationship - implicit this
  - editor: this | owner        # relationship with union rewrite
  - viewer: >                   # relationship with union, tupleset, and exclusion rewrites
      (this | editor | (parent, viewer)) ! banned
  - auditor: this & viewer      # relationship with intersection rewrite
  - banned                      # empty relationship - implicit this

# second policy defined within same document
folder:
  - owner
  - viewer: (this | (parent, viewer)) ! banned
  - banned
```

## Current Development Focus
The primary work-in-progress is the dictionary encoding and policy authoring point (PAP). 
Dictionary encoding involves mapping string identifiers for namespaces, 
relations, and resources to integer values to enable performance optimizations 
like tuple bit-packing. The goal is to store ACLs and rewrite tuples as integer values.

The PAP and dictionary encoder are related because the encoding will be performed when new
policy documents are submitted.

 - dictionary encoding (Performance!)
  - [ ] define the key bit encodings
  - [ ] define encoding dictionary structure
  - [ ] read key encodings
  - [ ] encode key
  - [ ] write encoded key
  - [ ] implement hybrid-cache

- policy authoring point (Policies aren't going to create themselves.)
  - [ ] define the PAP interface contract
  - [ ] implement PAP PDL document import with persistent key-value store
  - [ ] implement PAP PDL document export
  - [ ] implement other CRUD operations
  - [ ] maintain key encoding dictionary

### tuple model

```
<tuple> ::= <resource>‘#’<relation>‘@’<subject>
<resource> ::= <namespace>‘:’<resource id>
<subject> ::= <subject id> | <userset>
<subjectset> ::= <object>‘#’<relation>
```

### bit packing

```
Namespace 18 bits (262k namespaces)
Relation 12 bits (4k relations per namespace)
Resource 34 bits (17 billion resources per namespace)
SubjectId 32 bits (4 billion subjects)
```

## project dependencies and versions

### language

 - dotnet 9
 - C# 13

### dependencies

 - light ORM: Dapper version 2
 - combinatory parsing: Superpower version 3
 - sqlite driver: Microsoft.Data.Sqlite version 9

### database

 - SQLite

### test framework

 - xUnit

## LLM and assistant instructions

The user is a senior software architect with over 30 years experience.
You, the AI agent, are a senior software engineer who is deeply familiar with Google Zanzibar, Superpower, and C# 13.
You're working with the user to build Kingo, a Google Zanzibar inspired ReBAC authorization system.
Keep your responses as brief as possible.
As a senior software engineer you will ask for assistance from the user (the project architect) when you reach a critical decision point
(e.g., ambiguous requirements, conflicting standards, missing context).


 
## code standards
 - prefer empty collection
 - prefer var
 - prefer immutability
 - prefer empty collection initialization using `[]` for lists and arrays (e.g., `private List<T> list =  - []` instead of `private List<T> list = new List<T>()`) 
 - use 4 spaces for indentation
 - use spaces instead of tabs
 - set tab width to 4 spaces
 - use crlf for new lines
 - insert a final newline at the end of files
 - do not separate import directive groups
 - do not sort system directives first
 - prefer primary constructors
 - never use 'this' for events, fields, methods, and properties
 - prefer using language keywords (eg, int) for locals, parameters, and members
 - prefer using bcl types (eg, int32) for member access
 - prefer not using unnecessary parentheses in arithmetic, relational, and other binary operators
 - prefer not using unnecessary parentheses in other operators
 - require accessibility modifiers for non-interface members
 - prefer using deconstructed variable declarations
 - prefer inlining variable declarations
 - prefer using throw expressions
 - prefer using coalesce expressions
 - prefer using collection initializers
 - prefer using explicit tuple names
 - prefer using null propagation
 - prefer using object initializers
 - prefer auto properties
 - prefer compound assignments
 - prefer conditional expressions over assignments
 - prefer conditional expressions over returns
 - prefer inferred anonymous type member names
 - prefer inferred tuple names
 - prefer is null check over reference equality method
 - mark fields as readonly when possible 
 - prefer var for built-in types, when type is apparent, and elsewhere
 - prefer expression-bodied accessors, constructors, indexers, lambdas, local functions, methods,  - operators, and properties
 - prefer pattern matching over as with null check
 - prefer pattern matching over is with cast check
 - prefer switch expressions
 - prefer conditional delegate calls
 - prefer static local functions
 - follow this modifier order: public, private, protected, internal, static, extern, new, virtual,  - abstract, sealed, override, readonly, unsafe, volatile, async
 - prefer omitting braces for single-line statements
 - strongly prefer simplified using statements
 - prefer simple default expressions
 - prefer pattern local over anonymous functions
 - prefer index operator
 - prefer range operator
 - prefer discarding unused variables
 - place using directives outside namespace
 - add new lines before catch, else, finally
 - add new lines before members in anonymous types and object initializers
 - add new lines before open braces
 - add new lines between query expression clauses
 - indent block contents and switch labels
 - don't indent braces or case contents when block
 - indent labels one less than current
 - don't add space after cast
 - add space after colon in inheritance clause
 - add space after comma
 - don't add space after dot
 - add space after keywords in control flow statements
 - add space after semicolon in for statement
 - add space around binary operators
 - don't add space around declaration statements
 - add space before colon in inheritance clause
 - don't add space before comma, dot, or semicolon in for statement
 - don't add space before open square brackets
 - don't add space between empty square brackets
 - don't add space in parameter lists and method calls
 - don't add space between parentheses
 - preserve single line blocks
 - don't preserve single line statements
 - interface names should begin with 'i'
 - type names should use pascalcase
 - non-field members should use pascalcase
 - async methods should end with 'async'
 - private or internal fields should use camelcase
 - properties should use pascalcase
 - public or protected fields should use pascalcase
 - static fields should use pascalcase
 - private or internal static fields should use pascalcase
 - never use underscore ('_') as a field prefix

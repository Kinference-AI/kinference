# Prose style

These rules apply to all text in this repository, including commit
messages, pull request descriptions, docs and review comments.

- Write one main claim per sentence.
- Put reasons and asides in their own sentences instead of inserting them
  mid-sentence with dashes, parentheses, or relative clauses. Don’t use
  em-dashes.
- Every pronoun, “this/that/these,” or summarizing noun (“the two halves,”
  “both,” “this approach”) must have a single, explicit antecedent in the
  immediately preceding sentence. If there’s any doubt, repeat the noun.
- Don’t end paragraphs with summary or wrap-up sentences. If a conclusion
  depends on an assumption, state the assumption.
- Use active voice and name the actor: the client, the operator, the
  router. Inanimate subjects are fine with project terms such as
  "a worker set declares an alias".
- Keep the relativizer in object relative clauses: "the name that the
  client sent", not "the name the client sent".
- Use the project's own terms. Don't swap in metaphors such as
  "carries" for "declares".
- Name the concrete thing: "alias" or "model name", not just "name".
- Avoid noun stacks: "each endpoint's predicted time to first token",
  not "endpoint TTFT prediction metric".
- Keep words that depend on each other close: a verb near its subject
  and its complements, a modifier near the word it modifies. Move a
  long relative clause to the end of the sentence, or turn it into a
  leading condition: "when only one worker set declares a name, the
  router sends requests for that name to that set". Split a sentence
  whose one subject governs several far-apart verbs.


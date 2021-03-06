/*BEGIN_COPYRIGHT_BLOCK
 *
 * Copyright (c) 2001-2010, JavaPLT group at Rice University (drjava@rice.edu)
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the names of DrJava, the JavaPLT group, Rice University, nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software is Open Source Initiative approved Open Source Software.
 * Open Source Initative Approved is a trademark of the Open Source Initiative.
 * 
 * This file is part of DrJava.  Download the current version of this project
 * from http://www.drjava.org/ or http://sourceforge.net/projects/drjava/
 * 
 * END_COPYRIGHT_BLOCK*/

package edu.rice.cs.drjava.model;

import java.util.Collection;
import java.util.TreeSet;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

public class RegionSet<R extends IDocumentRegion> extends TreeSet<R> {

    // TODO: currently this assumes that a given RegionSet will only have 
    // regions from one document
    private DocumentListener _docListener = null;

    /** 
     * Set _docListener to listen on region.getDocument(), if not already set. 
     * @param region the region whose document should be listened on
     */
    private void _setDocListener(R region) {

      if (this._docListener != null) {
          return;
      }

      OpenDefinitionsDocument odd = region.getDocument();

      /* Listen on changes to the document, as these may affect the region. */
      final RegionSet<R> thisRef = this;
      _docListener = new DocumentListener() {

        public void insertUpdate(DocumentEvent e) {
          /* Insertion can't cause positions to flip */
          return;
        }

        public void removeUpdate(DocumentEvent e) {

          /* 
           * Removal can cause positions to flip, but only need to worry if 
           * the removed portion is within the bounds of one or more regions. 
           */
          boolean requireRebalance = false;
          for (R region : thisRef) {
            if (region.getStartOffset() <= e.getOffset() &&
              e.getOffset() <= region.getEndOffset()) {
                 requireRebalance = true;
                 break;
            }
          }

          if (requireRebalance) {
            @SuppressWarnings("unchecked")
            RegionSet<R> thisCopy = (RegionSet<R>)thisRef.clone();
            thisRef.removeAll(thisCopy);
            thisRef.addAll(thisCopy);
          }
        }

        public void changedUpdate(DocumentEvent e) {
          /* Apparently not used for documents. */
          return;
        }
      };
      odd.addDocumentListener(_docListener);
    }

    /** 
     * Adds an input region to the set.
     * @param region the region to add
     * @return indication of success
     */
    public boolean add(R region) {
        this._setDocListener(region);

        @SuppressWarnings("unchecked")
        RegionSet<IDocumentRegion> thisGeneric = (RegionSet<IDocumentRegion>)this;
        region.addSet(thisGeneric);
        return super.add(region);
    }

    /** 
     * Adds all input regions to the set.
     * @param regions the regions to add
     * @return indication of success
     */
    public boolean addAll(Collection<? extends R> regions) {
        @SuppressWarnings("unchecked")
        RegionSet<IDocumentRegion> thisGeneric = (RegionSet<IDocumentRegion>)this;
        for (R region : regions) {
            this._setDocListener(region);
            region.addSet(thisGeneric);
        }
        return super.addAll(regions);
    }

    /** 
     * Removes all existing regions from the set.
     */
    public void clear() {
        @SuppressWarnings("unchecked")
        RegionSet<IDocumentRegion> thisGeneric = (RegionSet<IDocumentRegion>)this;
        for (R region : this) {
            region.removeSet(thisGeneric);
        }
        super.clear();
    }

    /** 
     * Removes an input region from the set.
     * @param region the region to add
     * @return indication of success
     */
    public boolean remove(R region) {
        @SuppressWarnings("unchecked")
        RegionSet<IDocumentRegion> thisGeneric = (RegionSet<IDocumentRegion>)this;
        region.removeSet(thisGeneric);
        return super.remove(region);
    }
}